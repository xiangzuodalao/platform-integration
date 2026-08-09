from __future__ import annotations

import json
import os
import re
from typing import Annotated, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError


CREDENTIAL_REFERENCE_PATTERN = r"^[A-Z][A-Z0-9_]{2,127}$"
CREDENTIAL_REFERENCE_RE = re.compile(CREDENTIAL_REFERENCE_PATTERN)
CredentialReference = Annotated[str, Field(pattern=CREDENTIAL_REFERENCE_PATTERN)]


class CredentialResolutionError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class OpaqueBearerCredential(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    kind: Literal["opaque_bearer"]
    value: SecretStr = Field(min_length=1)


class ThingsBoardBearerCredential(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    kind: Literal["thingsboard_bearer"]
    value: SecretStr = Field(min_length=1)


class CmmsApiKeyCredential(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    kind: Literal["cmms_api_key"]
    value: SecretStr = Field(min_length=1)


class CmmsBearerCredential(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    kind: Literal["cmms_bearer"]
    value: SecretStr = Field(min_length=1)


CredentialEnvelope: TypeAlias = (
    OpaqueBearerCredential
    | ThingsBoardBearerCredential
    | CmmsApiKeyCredential
    | CmmsBearerCredential
)


class EnvironmentCredentialProvider:
    def get(self, reference: str) -> CredentialEnvelope:
        if type(reference) is not str or CREDENTIAL_REFERENCE_RE.fullmatch(reference) is None:
            raise CredentialResolutionError("CREDENTIAL_REFERENCE_INVALID") from None
        envelope = os.environ.get(reference)
        if envelope is None:
            raise CredentialResolutionError("CREDENTIAL_NOT_FOUND") from None
        invalid_envelope = False
        try:
            # Dispatch only on the exact kind token; each concrete model then
            # enforces strict types and rejects extra envelope fields.
            decoded = json.loads(envelope)
            if type(decoded) is not dict:
                raise ValueError("object envelope required")
            kind = decoded.get("kind")
            model = {
                "opaque_bearer": OpaqueBearerCredential,
                "thingsboard_bearer": ThingsBoardBearerCredential,
                "cmms_api_key": CmmsApiKeyCredential,
                "cmms_bearer": CmmsBearerCredential,
            }.get(kind)
            if model is None:
                raise ValueError("unsupported credential kind")
            result = model.model_validate(decoded)
            del decoded, kind, model
        except (ValidationError, ValueError):
            invalid_envelope = True
        if invalid_envelope:
            del envelope
            raise CredentialResolutionError("CREDENTIAL_ENVELOPE_INVALID") from None
        return result
