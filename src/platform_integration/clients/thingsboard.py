from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Any
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from platform_integration.credentials import (
    CREDENTIAL_REFERENCE_RE,
    CredentialResolutionError,
    EnvironmentCredentialProvider,
)
from platform_integration.services.data_quality import TelemetryPoint


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


@dataclass(frozen=True)
class ThingsBoardUserIdentity:
    user_id: UUID
    tenant_id: UUID


@dataclass(frozen=True)
class ThingsBoardAlarm:
    alarm_id: UUID
    tenant_id: UUID
    device_id: UUID
    alarm_type: str
    severity: str
    status: str
    details: dict[str, Any]
    update_fields: dict[str, Any]


def _entity_uuid(payload: object, field: str) -> UUID:
    if type(payload) is not dict:
        raise ValueError(f"{field} entity required")
    return UUID(str(_canonical_uuid(payload.get("id"))))


def _project_alarm(payload: object, *, for_update: bool = False) -> ThingsBoardAlarm:
    if type(payload) is not dict:
        raise ValueError("alarm object required")
    details = payload.get("details")
    if type(details) is not dict:
        raise ValueError("alarm details required")
    alarm_type = payload.get("type")
    severity = payload.get("severity")
    status = payload.get("status")
    if not all(type(value) is str and value for value in (alarm_type, severity, status)):
        raise ValueError("alarm scalars required")
    update_fields: dict[str, Any] = {}
    if for_update:
        boolean_fields = (
            "acknowledged",
            "cleared",
            "propagate",
            "propagateToOwner",
            "propagateToTenant",
        )
        integer_fields = ("startTs", "endTs", "ackTs", "clearTs", "assignTs")
        for field in boolean_fields:
            value = payload.get(field)
            if type(value) is not bool:
                raise ValueError("complete alarm update state required")
            update_fields[field] = value
        for field in integer_fields:
            value = payload.get(field)
            if type(value) is not int or value < 0:
                raise ValueError("complete alarm update state required")
            update_fields[field] = value
        relation_types = payload.get("propagateRelationTypes")
        if relation_types is not None and (
            type(relation_types) is not list
            or any(type(item) is not str for item in relation_types)
        ):
            raise ValueError("complete alarm update state required")
        update_fields["propagateRelationTypes"] = relation_types
        assignee = payload.get("assigneeId")
        if assignee is not None:
            _entity_uuid(assignee, "assignee")
            update_fields["assigneeId"] = assignee
        if update_fields["acknowledged"] != status.endswith("_ACK"):
            raise ValueError("alarm acknowledgement state mismatch")
        if update_fields["cleared"] != status.startswith("CLEARED"):
            raise ValueError("alarm cleared state mismatch")
    return ThingsBoardAlarm(
        alarm_id=_entity_uuid(payload.get("id"), "alarm id"),
        tenant_id=_entity_uuid(payload.get("tenantId"), "tenant id"),
        device_id=_entity_uuid(payload.get("originator"), "originator"),
        alarm_type=alarm_type,
        severity=severity,
        status=status,
        details=details,
        update_fields=update_fields,
    )


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

    async def alarm_info(self, alarm_id: UUID | str) -> ThingsBoardAlarm:
        canonical_id = str(_canonical_uuid(alarm_id))
        response = await self._request("GET", f"/api/alarm/info/{canonical_id}")
        try:
            return _project_alarm(response.json())
        except ValueError:
            raise ThingsBoardClientError("THINGSBOARD_INVALID_ALARM_RESPONSE") from None

    async def find_pdm_alarm(
        self,
        device_id: UUID | str,
        *,
        risk_key: str,
        alert_id: UUID | str,
        equipment_id: UUID | str,
        meas_code: str,
    ) -> ThingsBoardAlarm | None:
        canonical_id = str(_canonical_uuid(device_id))
        response = await self._request(
            "GET",
            f"/api/v2/alarm/DEVICE/{canonical_id}",
            params={
                "pageSize": 100,
                "page": 0,
                "statusList": "ACTIVE",
                "typeList": "PDM_FORECAST_RISK",
            },
        )
        try:
            payload = response.json()
            if type(payload) is not dict or type(payload.get("data")) is not list:
                raise ValueError("alarm page required")
            if payload.get("hasNext") is not False or payload.get("totalElements") != len(
                payload["data"]
            ):
                raise ValueError("complete alarm page required")
            canonical_alert_id = str(_canonical_uuid(alert_id))
            canonical_equipment_id = str(_canonical_uuid(equipment_id))
            matches = []
            for item in payload["data"]:
                alarm = _project_alarm(item, for_update=True)
                if (
                    alarm.details.get("risk_key") == risk_key
                    and alarm.details.get("alert_id") == canonical_alert_id
                    and alarm.details.get("equipment_id") == canonical_equipment_id
                    and alarm.details.get("meas_code") == meas_code
                ):
                    matches.append(alarm)
            if len(matches) > 1:
                raise ValueError("ambiguous alarm identity")
            return matches[0] if matches else None
        except ValueError:
            raise ThingsBoardClientError("THINGSBOARD_INVALID_ALARM_RESPONSE") from None

    async def upsert_pdm_alarm(
        self,
        *,
        device_id: UUID | str,
        details: dict[str, object],
        existing_alarm: ThingsBoardAlarm | None,
    ) -> UUID:
        canonical_device_id = str(_canonical_uuid(device_id))
        payload: dict[str, object] = {
            "originator": {"entityType": "DEVICE", "id": canonical_device_id},
            "type": "PDM_FORECAST_RISK",
            "severity": "WARNING",
            "propagate": False,
            "details": details,
        }
        if existing_alarm is not None:
            payload["id"] = {
                "entityType": "ALARM",
                "id": str(_canonical_uuid(existing_alarm.alarm_id)),
            }
            payload.update(existing_alarm.update_fields)
        response = await self._request("POST", "/api/alarm", json=payload, write=True)
        try:
            result = response.json()
            return _entity_uuid(result.get("id") if type(result) is dict else None, "alarm id")
        except ValueError:
            raise ThingsBoardClientError("THINGSBOARD_INVALID_ALARM_RESPONSE") from None

    async def clear_alarm(self, alarm_id: UUID | str) -> None:
        canonical_id = str(_canonical_uuid(alarm_id))
        await self._request("POST", f"/api/alarm/{canonical_id}/clear", write=True)

    async def historical_telemetry(
        self,
        device_id: UUID | str,
        *,
        telemetry_key: str,
        unit: str,
        start_ms: int,
        end_exclusive_ms: int,
    ) -> tuple[TelemetryPoint, ...]:
        canonical_id = str(_canonical_uuid(device_id))
        if (
            type(start_ms) is not int
            or type(end_exclusive_ms) is not int
            or end_exclusive_ms <= start_ms
        ):
            raise ThingsBoardClientError("THINGSBOARD_INVALID_TELEMETRY_WINDOW")
        response = await self._request(
            "GET",
            f"/api/plugins/telemetry/DEVICE/{canonical_id}/values/timeseries",
            params={
                "keys": telemetry_key,
                "startTs": start_ms,
                "endTs": end_exclusive_ms - 1,
                "interval": 60000,
                "agg": "AVG",
                "orderBy": "ASC",
            },
        )
        try:
            payload = response.json()
            if type(payload) is not dict or set(payload) != {telemetry_key}:
                raise ValueError("exact telemetry response required")
            rows = payload[telemetry_key]
            if type(rows) is not list:
                raise ValueError("telemetry rows required")
            points: list[TelemetryPoint] = []
            for row in rows:
                if type(row) is not dict or set(row) != {"ts", "value"}:
                    raise ValueError("strict telemetry row required")
                if type(row["ts"]) is not int or type(row["value"]) is not str:
                    raise ValueError("strict telemetry scalar required")
                points.append(TelemetryPoint(row["ts"], row["value"], unit))
            return tuple(points)
        except ValueError:
            raise ThingsBoardClientError("THINGSBOARD_INVALID_TELEMETRY_RESPONSE") from None

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


class BrowserThingsBoardClient:
    """ThingsBoard reads authorized by the exact browser token for one API request."""

    def __init__(self, *, http: httpx.AsyncClient) -> None:
        self._http = http

    async def _get(self, path: str, authorization: str) -> httpx.Response:
        if (
            type(authorization) is not str
            or len(authorization) > 8192
            or not authorization.startswith("Bearer ")
            or len(authorization) <= len("Bearer ")
        ):
            raise ThingsBoardClientError("THINGSBOARD_BROWSER_AUTH_INVALID")
        try:
            response = await self._http.get(
                path,
                headers={"X-Authorization": authorization},
                timeout=THINGSBOARD_TIMEOUT_SECONDS,
            )
        except httpx.HTTPError as exc:
            try:
                _scrub_request(exc.request)
            except RuntimeError:
                pass
            raise ThingsBoardClientError("THINGSBOARD_UNAVAILABLE") from None
        try:
            _scrub_request(response.request)
        except RuntimeError:
            pass
        if response.status_code == 401:
            raise ThingsBoardClientError("THINGSBOARD_BROWSER_AUTH_REJECTED")
        if response.status_code == 403:
            raise ThingsBoardClientError("THINGSBOARD_ALARM_FORBIDDEN")
        if response.status_code != 200:
            raise ThingsBoardClientError("THINGSBOARD_REQUEST_FAILED")
        return response

    async def authenticated_user(self, authorization: str) -> ThingsBoardUserIdentity:
        response = await self._get("/api/auth/user", authorization)
        try:
            payload = response.json()
            if type(payload) is not dict:
                raise ValueError("user object required")
            return ThingsBoardUserIdentity(
                user_id=_entity_uuid(payload.get("id"), "user id"),
                tenant_id=_entity_uuid(payload.get("tenantId"), "tenant id"),
            )
        except ValueError:
            raise ThingsBoardClientError("THINGSBOARD_INVALID_IDENTITY_RESPONSE") from None

    async def alarm_info(
        self,
        alarm_id: UUID | str,
        authorization: str,
    ) -> ThingsBoardAlarm:
        response = await self._get(
            f"/api/alarm/info/{str(_canonical_uuid(alarm_id))}", authorization
        )
        try:
            return _project_alarm(response.json())
        except ValueError:
            raise ThingsBoardClientError("THINGSBOARD_INVALID_ALARM_RESPONSE") from None
