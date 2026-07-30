from __future__ import annotations

import httpx
from pydantic import ValidationError

from platform_integration.contracts.pdm import (
    PdmContractError,
    PredictionRequestV2,
    PredictionResponseV2,
)
from platform_integration.credentials import (
    CREDENTIAL_REFERENCE_RE,
    CredentialResolutionError,
    EnvironmentCredentialProvider,
)


PDM_TIMEOUT_SECONDS = 10.0


class PdmClientError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _validated_wire_snapshot(
    request: PredictionRequestV2,
) -> tuple[PredictionRequestV2, dict[str, object]]:
    invalid_request = False
    wire_values: dict[str, object] | None = None
    try:
        wire_values = request.model_dump(mode="json", warnings="error")
        snapshot = PredictionRequestV2.model_validate(wire_values)
    except (ValidationError, ValueError):
        invalid_request = True
    if invalid_request:
        del request, wire_values
        raise PdmClientError("PDM_INVALID_REQUEST") from None
    payload = snapshot.model_dump(mode="json")
    del request, wire_values
    return snapshot, payload


def _scrub_authorization(request: httpx.Request) -> None:
    request.headers.pop("Authorization", None)


def _scrub_error_authorization(error: httpx.HTTPError) -> None:
    try:
        request = error.request
    except RuntimeError:
        return
    _scrub_authorization(request)


def _scrub_response_authorization(response: httpx.Response) -> None:
    for item in (*response.history, response):
        try:
            request = item.request
        except RuntimeError:
            continue
        _scrub_authorization(request)


class PdmClient:
    def __init__(
        self,
        *,
        http: httpx.AsyncClient,
        credentials: EnvironmentCredentialProvider,
        pdm_credential_ref: str | None,
    ) -> None:
        if pdm_credential_ref is None:
            raise PdmClientError("PDM_CREDENTIAL_REF_REQUIRED")
        if CREDENTIAL_REFERENCE_RE.fullmatch(pdm_credential_ref) is None:
            raise PdmClientError("PDM_CREDENTIAL_REF_INVALID")
        self._http = http
        self._credentials = credentials
        self._pdm_credential_ref = pdm_credential_ref

    async def predict(self, request: PredictionRequestV2) -> PredictionResponseV2:
        wire_request, wire_payload = _validated_wire_snapshot(request)
        del request
        credential_unavailable = False
        try:
            credential = self._credentials.get(self._pdm_credential_ref)
        except CredentialResolutionError:
            credential_unavailable = True
        if credential_unavailable:
            raise PdmClientError("PDM_CREDENTIAL_UNAVAILABLE") from None
        token = credential.value.get_secret_value()
        timed_out = False
        network_failed = False
        try:
            response = await self._http.post(
                "/api/v2/predictions",
                headers={"Authorization": f"Bearer {token}"},
                json=wire_payload,
                timeout=PDM_TIMEOUT_SECONDS,
            )
        except httpx.TimeoutException as exc:
            _scrub_error_authorization(exc)
            timed_out = True
        except httpx.HTTPError as exc:
            _scrub_error_authorization(exc)
            network_failed = True
        del credential, token
        if timed_out or network_failed:
            raise PdmClientError("PDM_UNAVAILABLE") from None
        _scrub_response_authorization(response)
        status_failed = False
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError:
            status_failed = True
        if status_failed:
            raise PdmClientError("PDM_REQUEST_FAILED") from None
        invalid_response = False
        try:
            result = PredictionResponseV2.model_validate(response.json())
        except (ValidationError, ValueError):
            invalid_response = True
        if invalid_response:
            raise PdmClientError("PDM_INVALID_RESPONSE") from None
        identity_error: str | None = None
        try:
            wire_request.assert_matching_response(result)
        except PdmContractError as exc:
            identity_error = exc.code
        if identity_error is not None:
            raise PdmClientError(identity_error) from None
        return result
