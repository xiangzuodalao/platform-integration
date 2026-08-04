from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any
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


class CmmsWorkOrder(_StrictModel):
    id: int = Field(gt=0)
    status: str = Field(min_length=1, max_length=48)
    event_version: int = Field(ge=0)
    updated_at: datetime
    external_source: str
    external_ref: CmmsUuid
    correlation_id: CmmsUuid
    equipment_id: CmmsUuid
    tb_alarm_id: CmmsUuid
    model_profile_id: str
    model_info_id: str
    policy_version: str
    asset_id: int = Field(gt=0)

    @field_validator("external_ref", "correlation_id", "equipment_id", "tb_alarm_id", mode="before")
    @classmethod
    def require_hyphenated_uuid(cls, value: object) -> object:
        return normalize_cmms_uuid(value)

    @field_validator("updated_at", mode="before")
    @classmethod
    def require_rfc3339_datetime(cls, value: object) -> object:
        if isinstance(value, datetime):
            parsed = value
        elif type(value) is str:
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                raise ValueError("RFC3339 datetime required") from None
        else:
            raise ValueError("RFC3339 datetime required")
        if parsed.utcoffset() is None:
            raise ValueError("timezone-aware datetime required")
        return parsed

    @classmethod
    def from_provider(cls, payload: Mapping[str, Any]) -> CmmsWorkOrder:
        asset = payload.get("asset")
        asset_id = asset.get("id") if isinstance(asset, Mapping) else payload.get("asset_id")
        return cls.model_validate(
            {
                "id": payload.get("id"),
                "status": payload.get("status"),
                "event_version": payload.get("event_version"),
                "updated_at": payload.get("updated_at", payload.get("updatedAt")),
                "external_source": payload.get("external_source"),
                "external_ref": payload.get("external_ref"),
                "correlation_id": payload.get("correlation_id"),
                "equipment_id": payload.get("equipment_id"),
                "tb_alarm_id": payload.get("tb_alarm_id"),
                "model_profile_id": payload.get("model_profile_id"),
                "model_info_id": payload.get("model_info_id"),
                "policy_version": payload.get("policy_version"),
                "asset_id": asset_id,
            }
        )
