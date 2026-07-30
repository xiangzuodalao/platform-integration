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


CMMS_TIMEOUT_SECONDS = 10.0
IDEMPOTENCY_KEY_RE = re.compile(
    r"^pilot-asset:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}$"
)


class CmmsClientError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class CmmsClient:
    def __init__(self, *, http: httpx.AsyncClient) -> None:
        self._http = http

    async def find_asset_by_equipment_id(self, equipment_id: UUID | str) -> CmmsAsset | None:
        canonical_id = str(normalize_cmms_uuid(equipment_id))
        request_failed = False
        try:
            response = await self._http.get(
                f"/api/assets/by-equipment-id/{canonical_id}",
                timeout=CMMS_TIMEOUT_SECONDS,
            )
        except httpx.HTTPError:
            request_failed = True
        if request_failed:
            raise CmmsClientError("CMMS_UNAVAILABLE") from None
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
        timed_out = False
        request_failed = False
        try:
            response = await self._http.post(
                "/api/assets",
                headers={"Idempotency-Key": idempotency_key},
                json=request.model_dump(mode="json"),
                timeout=CMMS_TIMEOUT_SECONDS,
            )
        except httpx.TimeoutException:
            timed_out = True
        except httpx.HTTPError:
            request_failed = True
        if timed_out:
            raise CmmsClientError("CMMS_WRITE_RESULT_UNKNOWN") from None
        if request_failed:
            raise CmmsClientError("CMMS_UNAVAILABLE") from None
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
