from __future__ import annotations

import os
import re
from typing import Annotated, Literal

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


class EnvironmentCredentialProvider:
    def get(self, reference: str) -> OpaqueBearerCredential:
        if type(reference) is not str or CREDENTIAL_REFERENCE_RE.fullmatch(reference) is None:
            raise CredentialResolutionError("CREDENTIAL_REFERENCE_INVALID") from None
        envelope = os.environ.get(reference)
        if envelope is None:
            raise CredentialResolutionError("CREDENTIAL_NOT_FOUND") from None
        invalid_envelope = False
        try:
            result = OpaqueBearerCredential.model_validate_json(envelope)
        except (ValidationError, ValueError):
            invalid_envelope = True
        if invalid_envelope:
            del envelope
            raise CredentialResolutionError("CREDENTIAL_ENVELOPE_INVALID") from None
        return result
