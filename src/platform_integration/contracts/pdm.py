from __future__ import annotations

import re
from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


CANONICAL_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
DECIMAL_RE = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
RFC3339_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
CanonicalUuid = Annotated[UUID, Field(strict=False)]


class PdmContractError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _canonical_uuid(value: object) -> object:
    if isinstance(value, UUID):
        return value
    if type(value) is not str or CANONICAL_UUID_RE.fullmatch(value) is None:
        raise ValueError("canonical lowercase hyphenated UUID required")
    return value


def _integer_token(value: int) -> int:
    if type(value) is not int:
        raise ValueError("integer JSON token required")
    return value


def _decimal_string(value: str) -> str:
    if type(value) is not str or DECIMAL_RE.fullmatch(value) is None:
        raise ValueError("finite non-exponent decimal string required")
    return value


def _sha256(value: str) -> str:
    if type(value) is not str or SHA256_RE.fullmatch(value) is None:
        raise ValueError("lowercase SHA-256 required")
    return value


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class HistoryPointV2(_StrictModel):
    data_id: str = Field(min_length=1)
    timestamp: int = Field(ge=0)
    value: str
    unit: str = Field(min_length=1)

    @field_validator("timestamp")
    @classmethod
    def require_integer_token(cls, value: int) -> int:
        return _integer_token(value)

    @field_validator("value")
    @classmethod
    def require_decimal_string(cls, value: str) -> str:
        return _decimal_string(value)


class ForecastPointV2(_StrictModel):
    timestamp: int = Field(ge=0)
    value: str
    unit: str = Field(min_length=1)

    @field_validator("timestamp")
    @classmethod
    def require_integer_token(cls, value: int) -> int:
        return _integer_token(value)

    @field_validator("value")
    @classmethod
    def require_decimal_string(cls, value: str) -> str:
        return _decimal_string(value)


class PredictionResponseV2(_StrictModel):
    correlation_id: CanonicalUuid
    equipment_id: CanonicalUuid
    model_profile_id: str = Field(min_length=1)
    model_info_id: str = Field(min_length=1)
    model_artifact_sha256: str
    meas_code: str = Field(min_length=1)
    request_digest: str
    input_digest: str
    generated_at: datetime
    forecast: list[ForecastPointV2] = Field(min_length=15, max_length=15)

    @field_validator("correlation_id", "equipment_id", mode="before")
    @classmethod
    def require_canonical_uuid_string(cls, value: object) -> object:
        return _canonical_uuid(value)

    @field_validator("model_artifact_sha256", "request_digest", "input_digest")
    @classmethod
    def require_digest(cls, value: str) -> str:
        return _sha256(value)

    @field_validator("generated_at", mode="before")
    @classmethod
    def require_rfc3339(cls, value: object) -> datetime:
        if isinstance(value, datetime):
            parsed = value
        else:
            if type(value) is not str or RFC3339_RE.fullmatch(value) is None:
                raise ValueError("RFC 3339 timestamp required")
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                raise ValueError("RFC 3339 timestamp required") from None
        if parsed.utcoffset() is None:
            raise ValueError("RFC 3339 timestamp requires an offset")
        return parsed


class PredictionRequestV2(_StrictModel):
    tenant_id: CanonicalUuid
    correlation_id: CanonicalUuid
    equipment_id: CanonicalUuid
    model_profile_id: str = Field(min_length=1)
    model_info_id: str = Field(min_length=1)
    meas_code: str = Field(min_length=1)
    unit: str = Field(min_length=1)
    sampling_frequency: str = Field(min_length=1)
    window_start: int = Field(ge=0)
    window_end: int = Field(ge=0)
    request_digest: str
    history: list[HistoryPointV2]

    @field_validator("tenant_id", "correlation_id", "equipment_id", mode="before")
    @classmethod
    def require_canonical_uuid_string(cls, value: object) -> object:
        return _canonical_uuid(value)

    @field_validator("window_start", "window_end")
    @classmethod
    def require_integer_token(cls, value: int) -> int:
        return _integer_token(value)

    @field_validator("request_digest")
    @classmethod
    def require_digest(cls, value: str) -> str:
        return _sha256(value)

    def assert_matching_response(self, response: PredictionResponseV2) -> None:
        expected = (
            self.correlation_id,
            self.equipment_id,
            self.model_profile_id,
            self.model_info_id,
            self.meas_code,
            self.request_digest,
        )
        actual = (
            response.correlation_id,
            response.equipment_id,
            response.model_profile_id,
            response.model_info_id,
            response.meas_code,
            response.request_digest,
        )
        if actual != expected:
            raise PdmContractError("PDM_RESPONSE_IDENTITY_MISMATCH")
