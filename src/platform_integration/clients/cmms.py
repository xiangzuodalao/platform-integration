from __future__ import annotations

import re
from uuid import UUID

import httpx
from pydantic import ValidationError

from platform_integration.contracts.cmms import (
    CmmsAsset,
    CmmsAssetCreate,
    normalize_cmms_uuid,
)
from platform_integration.credentials import (
    CREDENTIAL_REFERENCE_RE,
    CredentialResolutionError,
    EnvironmentCredentialProvider,
)


CMMS_TIMEOUT_SECONDS = 10.0
SIGNED_BIGINT_MAX = 9_223_372_036_854_775_807
IDEMPOTENCY_KEY_RE = re.compile(
    r"^pilot-asset:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}$"
)


class CmmsClientError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _scrub_request(request: httpx.Request) -> None:
    request.headers.pop("x-api-key", None)


class CmmsClient:
    def __init__(
        self,
        *,
        http: httpx.AsyncClient,
        credentials: EnvironmentCredentialProvider,
        cmms_credential_ref: str | None,
    ) -> None:
        if cmms_credential_ref is None:
            raise CmmsClientError("CMMS_CREDENTIAL_REF_REQUIRED")
        if CREDENTIAL_REFERENCE_RE.fullmatch(cmms_credential_ref) is None:
            raise CmmsClientError("CMMS_CREDENTIAL_REF_INVALID")
        self._http = http
        self._credentials = credentials
        self._credential_ref = cmms_credential_ref

    def _api_key(self) -> str:
        try:
            credential = self._credentials.get(self._credential_ref)
        except CredentialResolutionError:
            raise CmmsClientError("CMMS_CREDENTIAL_UNAVAILABLE") from None
        if credential.kind != "cmms_api_key":
            del credential
            raise CmmsClientError("CMMS_CREDENTIAL_KIND_INVALID") from None
        api_key = credential.value.get_secret_value()
        del credential
        return api_key

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        write: bool = False,
    ) -> httpx.Response:
        api_key = self._api_key()
        request_headers = {**(headers or {}), "x-api-key": api_key}
        failure_code: str | None = None
        try:
            response = await self._http.request(
                method,
                path,
                json=json,
                headers=request_headers,
                timeout=CMMS_TIMEOUT_SECONDS,
            )
        except httpx.TimeoutException as exc:
            _scrub_request(exc.request)
            failure_code = "CMMS_WRITE_RESULT_UNKNOWN" if write else "CMMS_UNAVAILABLE"
        except httpx.HTTPError as exc:
            try:
                _scrub_request(exc.request)
            except RuntimeError:
                pass
            failure_code = "CMMS_UNAVAILABLE"
        finally:
            del api_key, request_headers
        if failure_code is not None:
            raise CmmsClientError(failure_code) from None
        try:
            _scrub_request(response.request)
        except RuntimeError:
            pass
        return response

    async def authenticated_company_id(self) -> int:
        response = await self._request("GET", "/api/auth/me")
        if response.status_code != 200:
            raise CmmsClientError("CMMS_REQUEST_FAILED")
        try:
            payload = response.json()
            if type(payload) is not dict:
                raise ValueError("strict identity response required")
            company_id = payload["companyId"]
            if type(company_id) is not int or company_id <= 0 or company_id > SIGNED_BIGINT_MAX:
                raise ValueError("positive bigint required")
            return company_id
        except (KeyError, ValueError):
            raise CmmsClientError("CMMS_INVALID_IDENTITY_RESPONSE") from None

    async def total_work_orders(self) -> int:
        response = await self._request(
            "POST",
            "/api/work-orders/search",
            json={
                "filterFields": [],
                "direction": "ASC",
                "pageNum": 0,
                "pageSize": 1,
                "sortField": "id",
            },
        )
        if response.status_code != 200:
            raise CmmsClientError("CMMS_REQUEST_FAILED")
        try:
            payload = response.json()
            if type(payload) is not dict:
                raise ValueError("work-order page required")
            total = payload.get("totalElements")
            if type(total) is not int or total < 0:
                raise ValueError("nonnegative count required")
            return total
        except ValueError:
            raise CmmsClientError("CMMS_INVALID_WORK_ORDER_RESPONSE") from None

    async def find_asset_by_equipment_id(self, equipment_id: UUID | str) -> CmmsAsset | None:
        canonical_id = str(normalize_cmms_uuid(equipment_id))
        response = await self._request("GET", f"/api/assets/by-equipment-id/{canonical_id}")
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise CmmsClientError("CMMS_REQUEST_FAILED")
        return self._project_asset(response)

    async def create_asset(
        self,
        request: CmmsAssetCreate,
        *,
        idempotency_key: str,
    ) -> CmmsAsset:
        if (
            type(idempotency_key) is not str
            or IDEMPOTENCY_KEY_RE.fullmatch(idempotency_key) is None
        ):
            raise CmmsClientError("IDEMPOTENCY_KEY_INVALID")
        response = await self._request(
            "POST",
            "/api/assets",
            headers={"Idempotency-Key": idempotency_key},
            json=request.model_dump(mode="json"),
            write=True,
        )
        if response.status_code == 409:
            raise CmmsClientError("IDEMPOTENCY_CONFLICT")
        if response.status_code != 201:
            raise CmmsClientError("CMMS_REQUEST_FAILED")
        return self._project_asset(response)

    @staticmethod
    def _project_asset(response: httpx.Response) -> CmmsAsset:
        invalid_response = False
        try:
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("object response required")
            asset = CmmsAsset.from_provider(payload)
        except (ValidationError, ValueError):
            invalid_response = True
        if invalid_response:
            raise CmmsClientError("CMMS_INVALID_RESPONSE") from None
        return asset
