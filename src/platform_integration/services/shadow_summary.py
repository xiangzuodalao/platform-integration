from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select

from platform_integration.models.bindings import EquipmentMapping, TenantBinding
from platform_integration.models.prediction import PredictionRun


class ShadowSummaryError(ValueError):
    pass


def _rfc3339(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def build_shadow_summary(
    *,
    tenant_alias: str,
    tenant_id: UUID,
    scheduled_at: datetime,
    rows: list[dict[str, object]],
) -> dict[str, object]:
    if scheduled_at.utcoffset() is None:
        raise ShadowSummaryError("SHADOW_SUMMARY_SLOT_INVALID")
    utc_slot = scheduled_at.astimezone(UTC)
    if utc_slot.minute % 15 or utc_slot.second or utc_slot.microsecond:
        raise ShadowSummaryError("SHADOW_SUMMARY_SLOT_INVALID")
    if len(rows) > 100:
        raise ShadowSummaryError("SHADOW_SUMMARY_LIMIT_EXCEEDED")
    mappings = sorted(
        (
            {
                "equipment_id": str(row["equipment_id"]),
                "meas_code": row["meas_code"],
                "tb_device_id": str(row["tb_device_id"]),
                "cmms_asset_id": row["cmms_asset_id"],
            }
            for row in rows
        ),
        key=lambda item: (item["equipment_id"], item["meas_code"]),
    )
    models = sorted(
        {
            (str(row["model_profile_id"]), str(row["model_artifact_sha256"]))
            for row in rows
            if row.get("model_profile_id") is not None
            and row.get("model_artifact_sha256") is not None
        }
    )
    counts = Counter(str(row["status"]) for row in rows)
    return {
        "tenant_alias": tenant_alias,
        "tenant_id": str(tenant_id),
        "scheduled_at": _rfc3339(scheduled_at),
        "status_counts": dict(sorted(counts.items())),
        "mappings": mappings,
        "models": [
            {"model_profile_id": profile, "model_artifact_sha256": artifact}
            for profile, artifact in models
        ],
    }


class ShadowSummaryService:
    def __init__(self, *, sessions) -> None:
        self._sessions = sessions

    async def load(
        self,
        *,
        tenant_alias: str,
        scheduled_at: datetime,
        expected_tenant_id: UUID | None = None,
    ) -> dict[str, object]:
        async with self._sessions() as session:
            tenant = await session.scalar(
                select(TenantBinding).where(TenantBinding.alias == tenant_alias)
            )
            if tenant is None or (
                expected_tenant_id is not None and tenant.tenant_id != expected_tenant_id
            ):
                raise ShadowSummaryError("SHADOW_SUMMARY_TENANT_MISMATCH")
            result = await session.execute(
                select(PredictionRun, EquipmentMapping)
                .join(
                    EquipmentMapping,
                    (EquipmentMapping.tenant_id == PredictionRun.tenant_id)
                    & (EquipmentMapping.equipment_id == PredictionRun.equipment_id),
                )
                .where(
                    PredictionRun.tenant_id == tenant.tenant_id,
                    PredictionRun.scheduled_at == scheduled_at,
                )
                .order_by(PredictionRun.equipment_id, PredictionRun.meas_code)
                .limit(101)
            )
            rows = [
                {
                    "equipment_id": run.equipment_id,
                    "meas_code": run.meas_code,
                    "tb_device_id": equipment.tb_device_id,
                    "cmms_asset_id": equipment.cmms_asset_id,
                    "model_profile_id": run.model_profile_id,
                    "model_artifact_sha256": run.model_artifact_sha256,
                    "status": run.status,
                }
                for run, equipment in result.all()
            ]
        return build_shadow_summary(
            tenant_alias=tenant_alias,
            tenant_id=tenant.tenant_id,
            scheduled_at=scheduled_at,
            rows=rows,
        )
