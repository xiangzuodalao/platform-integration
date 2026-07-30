from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


CMMS_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
CmmsUuid = Annotated[UUID, Field(strict=False)]


def normalize_cmms_uuid(value: object) -> object:
    if isinstance(value, UUID):
        return value
    if type(value) is not str or CMMS_UUID_RE.fullmatch(value) is None:
        raise ValueError("hyphenated UUID required")
    return value.lower()


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class CmmsAssetCreate(_StrictModel):
    name: str = Field(min_length=1)
    equipment_id: CmmsUuid

    @field_validator("equipment_id", mode="before")
    @classmethod
    def require_hyphenated_uuid(cls, value: object) -> object:
        return normalize_cmms_uuid(value)


class CmmsAsset(_StrictModel):
    id: int = Field(gt=0)
    name: str = Field(min_length=1)
    equipment_id: CmmsUuid

    @field_validator("equipment_id", mode="before")
    @classmethod
    def require_hyphenated_uuid(cls, value: object) -> object:
        return normalize_cmms_uuid(value)

    @classmethod
    def from_provider(cls, payload: Mapping[str, object]) -> CmmsAsset:
        projection = {field: payload.get(field) for field in ("id", "name", "equipment_id")}
        return cls.model_validate(projection)
