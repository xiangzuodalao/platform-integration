from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from platform_integration.credentials import (
    CREDENTIAL_REFERENCE_RE,
    CredentialResolutionError,
    EnvironmentCredentialProvider,
)


THINGSBOARD_TIMEOUT_SECONDS = 10.0
CanonicalUuid = Annotated[UUID, Field(strict=False)]


class ThingsBoardClientError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _canonical_uuid(value: object) -> object:
    if isinstance(value, UUID):
        return value
    if type(value) is not str or str(UUID(value)) != value:
        raise ValueError("canonical UUID required")
    return value


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)


class ThingsBoardDevice(_StrictModel):
    id: CanonicalUuid
    name: str = Field(min_length=1)
    device_type: str = Field(alias="type", min_length=1)

    @field_validator("id", mode="before")
    @classmethod
    def validate_id(cls, value: object) -> object:
        return _canonical_uuid(value)

    @classmethod
    def from_provider(cls, value: object) -> ThingsBoardDevice:
        if type(value) is not dict:
            raise ValueError("device object required")
        identifier = value.get("id")
        if type(identifier) is not dict:
            raise ValueError("device id object required")
        return cls.model_validate(
            {
                "id": identifier.get("id"),
                "name": value.get("name"),
                "type": value.get("type"),
            }
        )


class ThingsBoardAssetAttributes(_StrictModel):
    equipment_id: CanonicalUuid
    cmms_asset_id: int = Field(gt=0)

    @field_validator("equipment_id", mode="before")
    @classmethod
    def validate_id(cls, value: object) -> object:
        return _canonical_uuid(value)

    @classmethod
    def from_provider(cls, value: object) -> ThingsBoardAssetAttributes:
        if type(value) is not list:
            raise ValueError("attribute list required")
        projected: dict[str, object] = {}
        for item in value:
            if type(item) is not dict:
                raise ValueError("attribute object required")
            key = item.get("key")
            if key in {"equipment_id", "cmms_asset_id"}:
                projected[key] = item.get("value")
        return cls.model_validate(projected)


def _scrub_request(request: httpx.Request) -> None:
    request.headers.pop("X-Authorization", None)


class ThingsBoardClient:
    def __init__(
        self,
        *,
        http: httpx.AsyncClient,
        credentials: EnvironmentCredentialProvider,
        tb_credential_ref: str | None,
    ) -> None:
        if tb_credential_ref is None:
            raise ThingsBoardClientError("THINGSBOARD_CREDENTIAL_REF_REQUIRED")
        if CREDENTIAL_REFERENCE_RE.fullmatch(tb_credential_ref) is None:
            raise ThingsBoardClientError("THINGSBOARD_CREDENTIAL_REF_INVALID")
        self._http = http
        self._credentials = credentials
        self._credential_ref = tb_credential_ref

    def _authorization(self) -> str:
        try:
            credential = self._credentials.get(self._credential_ref)
        except CredentialResolutionError:
            raise ThingsBoardClientError("THINGSBOARD_CREDENTIAL_UNAVAILABLE") from None
        if credential.kind != "thingsboard_bearer":
            del credential
            raise ThingsBoardClientError("THINGSBOARD_CREDENTIAL_KIND_INVALID") from None
        token = credential.value.get_secret_value()
        header = f"Bearer {token}"
        del credential, token
        return header

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, object] | None = None,
        json: Mapping[str, object] | None = None,
        write: bool = False,
    ) -> httpx.Response:
        authorization = self._authorization()
        try:
            response = await self._http.request(
                method,
                path,
                params=params,
                json=json,
                headers={"X-Authorization": authorization},
                timeout=THINGSBOARD_TIMEOUT_SECONDS,
            )
        except httpx.TimeoutException as exc:
            _scrub_request(exc.request)
            code = "THINGSBOARD_WRITE_RESULT_UNKNOWN" if write else "THINGSBOARD_UNAVAILABLE"
            raise ThingsBoardClientError(code) from None
        except httpx.HTTPError as exc:
            try:
                _scrub_request(exc.request)
            except RuntimeError:
                pass
            raise ThingsBoardClientError("THINGSBOARD_UNAVAILABLE") from None
        finally:
            del authorization
        try:
            _scrub_request(response.request)
        except RuntimeError:
            pass
        if response.status_code < 200 or response.status_code >= 300:
            raise ThingsBoardClientError("THINGSBOARD_REQUEST_FAILED")
        return response

    async def authenticated_tenant_id(self) -> UUID:
        response = await self._request("GET", "/api/auth/user")
        try:
            payload = response.json()
            if type(payload) is not dict:
                raise ValueError("strict identity response required")
            tenant_id = payload["tenantId"]
            if type(tenant_id) is not dict:
                raise ValueError("strict tenant id required")
            return UUID(str(_canonical_uuid(tenant_id["id"])))
        except (KeyError, TypeError, ValueError):
            raise ThingsBoardClientError("THINGSBOARD_INVALID_IDENTITY_RESPONSE") from None

    async def list_devices(self) -> tuple[ThingsBoardDevice, ...]:
        response = await self._request(
            "GET",
            "/api/tenant/devices",
            params={"pageSize": 100, "page": 0},
        )
        try:
            payload = response.json()
            if type(payload) is not dict:
                raise ValueError("device page required")
            data = payload.get("data")
            if type(data) is not list:
                raise ValueError("device list required")
            if payload.get("hasNext") is not False or payload.get("totalElements") != len(data):
                raise ValueError("complete device page required")
            return tuple(ThingsBoardDevice.from_provider(item) for item in data)
        except (ValidationError, ValueError):
            raise ThingsBoardClientError("THINGSBOARD_INVALID_DEVICE_RESPONSE") from None

    async def active_pdm_alarm_count(self, device_id: UUID | str) -> int:
        canonical_id = str(_canonical_uuid(device_id))
        response = await self._request(
            "GET",
            f"/api/v2/alarm/DEVICE/{canonical_id}",
            params={
                "pageSize": 1,
                "page": 0,
                "statusList": "ACTIVE",
                "typeList": "PDM_FORECAST_RISK",
            },
        )
        try:
            payload = response.json()
            if type(payload) is not dict:
                raise ValueError("alarm page required")
            count = payload.get("totalElements")
            if type(count) is not int or count < 0:
                raise ValueError("nonnegative count required")
            return count
        except ValueError:
            raise ThingsBoardClientError("THINGSBOARD_INVALID_ALARM_RESPONSE") from None

    async def write_asset_attributes(
        self,
        device_id: UUID | str,
        *,
        equipment_id: UUID | str,
        cmms_asset_id: int,
    ) -> None:
        canonical_device_id = str(_canonical_uuid(device_id))
        payload = ThingsBoardAssetAttributes(
            equipment_id=equipment_id,
            cmms_asset_id=cmms_asset_id,
        ).model_dump(mode="json")
        await self._request(
            "POST",
            f"/api/plugins/telemetry/DEVICE/{canonical_device_id}/attributes/SERVER_SCOPE",
            json=payload,
            write=True,
        )

    async def read_asset_attributes(self, device_id: UUID | str) -> ThingsBoardAssetAttributes:
        canonical_id = str(_canonical_uuid(device_id))
        response = await self._request(
            "GET",
            f"/api/plugins/telemetry/DEVICE/{canonical_id}/values/attributes/SERVER_SCOPE",
            params={"keys": "equipment_id,cmms_asset_id"},
        )
        try:
            return ThingsBoardAssetAttributes.from_provider(response.json())
        except (ValidationError, ValueError):
            raise ThingsBoardClientError("THINGSBOARD_INVALID_ATTRIBUTE_RESPONSE") from None
