from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID, uuid5

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from platform_integration.models.audit import AuditEvent
from platform_integration.models.bindings import (
    EquipmentMapping,
    MeasurementBinding,
    ProvisioningPlan as ProvisioningPlanRow,
    TenantBinding,
)
from platform_integration.services.provisioning import (
    ProvisioningError,
    ProvisioningPlan,
    ProvisioningTarget,
)
from platform_integration.services.tenant_bindings import ValidatedTenantBinding


def _target_from_canonical(value: dict[str, Any]) -> ProvisioningTarget:
    return ProvisioningTarget(
        tb_device_id=UUID(value["tb_device_id"]),
        device_name=value["device_name"],
        device_type=value["device_type"],
        asset_name=value["asset_name"],
        equipment_id=UUID(value["equipment_id"]),
        measurement_binding=value["measurement_binding"],
    )


def _plan_from_row(row: ProvisioningPlanRow) -> ProvisioningPlan:
    canonical = row.canonical_plan
    return ProvisioningPlan(
        tenant_id=row.tenant_id,
        plan_hash=row.plan_hash,
        canonical=canonical,
        tenant_snapshot=row.tenant_snapshot,
        targets=tuple(_target_from_canonical(value) for value in canonical["targets"]),
        actor=row.actor_id,
        created_at=row.created_at,
        expires_at=row.expires_at,
        applied_at=row.applied_at,
        target_results=row.target_results,
        terminal_result=row.terminal_result,
    )


def _persisted_measurement(value: dict[str, object]) -> dict[str, object]:
    return {key: item for key, item in value.items() if key != "predictor_kind"}


def _measurement_from_row(row: MeasurementBinding) -> dict[str, object]:
    return {
        "meas_code": row.meas_code,
        "telemetry_key": row.telemetry_key,
        "unit": row.unit,
        "sampling_frequency": row.sampling_frequency,
        "request_window_points": row.request_window_points,
        "context_points": row.context_points,
        "horizon_points": row.horizon_points,
        "value_scale": row.value_scale,
        "model_profile_id": row.model_profile_id,
        "model_info_id": row.model_info_id,
        "preprocessing_version": row.preprocessing_version,
        "policy_version": row.policy_version,
        "risk_direction": row.risk_direction,
        "risk_threshold": f"{row.risk_threshold:.{row.value_scale}f}",
        "enabled": row.enabled,
    }


class SqlProvisioningStore:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._apply_sessions: dict[tuple[UUID, str], AsyncSession] = {}

    async def save_plan(self, snapshot: ValidatedTenantBinding, plan: ProvisioningPlan) -> None:
        async with self._sessions() as session, session.begin():
            existing = await session.get(TenantBinding, snapshot.tenant_id)
            identity = (
                snapshot.alias,
                snapshot.tb_tenant_id,
                snapshot.cmms_company_id,
                snapshot.pdm_credential_ref,
                snapshot.tb_credential_ref,
                snapshot.cmms_credential_ref,
                snapshot.cmms_webhook_secret_ref,
            )
            if existing is None:
                session.add(
                    TenantBinding(
                        tenant_id=snapshot.tenant_id,
                        alias=snapshot.alias,
                        tb_tenant_id=snapshot.tb_tenant_id,
                        cmms_company_id=snapshot.cmms_company_id,
                        pdm_credential_ref=snapshot.pdm_credential_ref,
                        tb_credential_ref=snapshot.tb_credential_ref,
                        cmms_credential_ref=snapshot.cmms_credential_ref,
                        cmms_webhook_secret_ref=snapshot.cmms_webhook_secret_ref,
                        enabled=True,
                    )
                )
            else:
                persisted = (
                    existing.alias,
                    existing.tb_tenant_id,
                    existing.cmms_company_id,
                    existing.pdm_credential_ref,
                    existing.tb_credential_ref,
                    existing.cmms_credential_ref,
                    existing.cmms_webhook_secret_ref,
                )
                if persisted != identity:
                    raise ProvisioningError("TENANT_BINDING_CONFLICT")
                existing.enabled = True
            duplicate = await session.scalar(
                select(ProvisioningPlanRow.plan_id).where(
                    ProvisioningPlanRow.tenant_id == plan.tenant_id,
                    ProvisioningPlanRow.plan_hash == plan.plan_hash,
                )
            )
            if duplicate is not None:
                raise ProvisioningError("PROVISIONING_PLAN_ALREADY_EXISTS")
            session.add(
                ProvisioningPlanRow(
                    tenant_id=plan.tenant_id,
                    plan_hash=plan.plan_hash,
                    canonical_plan=plan.canonical,
                    tenant_snapshot=plan.tenant_snapshot,
                    tb_tenant_id=snapshot.tb_tenant_id,
                    cmms_company_id=snapshot.cmms_company_id,
                    actor_id=plan.actor,
                    created_at=plan.created_at,
                    expires_at=plan.expires_at,
                )
            )

    async def load_plan(self, plan_hash: str) -> ProvisioningPlan | None:
        async with self._sessions() as session:
            row = await session.scalar(
                select(ProvisioningPlanRow).where(ProvisioningPlanRow.plan_hash == plan_hash)
            )
            return None if row is None else _plan_from_row(row)

    async def preflight_plan(
        self,
        plan: ProvisioningPlan,
        reservations: tuple[tuple[ProvisioningTarget, str, dict[str, object]], ...],
    ) -> None:
        async with self._sessions() as session:
            for target, digest, summary in reservations:
                mapping = await session.scalar(
                    select(EquipmentMapping).where(
                        EquipmentMapping.tenant_id == plan.tenant_id,
                        EquipmentMapping.tb_device_id == target.tb_device_id,
                    )
                )
                if mapping is not None and (
                    mapping.equipment_id != target.equipment_id
                    or mapping.cmms_request_digest != digest
                    or mapping.cmms_request_summary != summary
                ):
                    raise ProvisioningError("EQUIPMENT_RESERVATION_CONFLICT")
                binding = await session.scalar(
                    select(MeasurementBinding).where(
                        MeasurementBinding.tenant_id == plan.tenant_id,
                        MeasurementBinding.equipment_id == target.equipment_id,
                        MeasurementBinding.meas_code == target.measurement_binding["meas_code"],
                    )
                )
                if binding is not None and _measurement_from_row(binding) != _persisted_measurement(
                    target.measurement_binding
                ):
                    raise ProvisioningError("MEASUREMENT_BINDING_CONFLICT")

    async def start_apply(self, plan: ProvisioningPlan, actor: UUID, now: Any) -> None:
        key = (plan.tenant_id, plan.plan_hash)
        if key in self._apply_sessions:
            raise ProvisioningError("PROVISIONING_APPLY_CONCURRENT")
        session = self._sessions()
        acquired = await session.scalar(
            text("SELECT pg_try_advisory_lock(hashtext(:tenant_id), hashtext(:plan_hash))"),
            {
                "tenant_id": str(plan.tenant_id),
                "plan_hash": plan.plan_hash,
            },
        )
        await session.commit()
        if acquired is not True:
            await session.close()
            raise ProvisioningError("PROVISIONING_APPLY_CONCURRENT")
        row = await session.scalar(
            select(ProvisioningPlanRow).where(
                ProvisioningPlanRow.tenant_id == plan.tenant_id,
                ProvisioningPlanRow.plan_hash == plan.plan_hash,
            )
        )
        await session.commit()
        if (
            row is None
            or row.applied_at is not None
            or row.terminal_result is not None
            or now >= row.expires_at
        ):
            await self._unlock_and_close(key, session)
            raise ProvisioningError("PROVISIONING_PLAN_NOT_APPLICABLE")
        self._apply_sessions[key] = session

    async def abort_apply(self, plan: ProvisioningPlan) -> None:
        key = (plan.tenant_id, plan.plan_hash)
        session = self._apply_sessions.pop(key, None)
        if session is not None:
            await self._unlock_and_close(key, session)

    async def reserve_target(
        self,
        plan: ProvisioningPlan,
        target: ProvisioningTarget,
        request_digest: str,
        request_summary: dict[str, object],
    ) -> None:
        async with self._sessions() as session, session.begin():
            mapping = await session.scalar(
                select(EquipmentMapping)
                .where(
                    EquipmentMapping.tenant_id == plan.tenant_id,
                    EquipmentMapping.tb_device_id == target.tb_device_id,
                )
                .with_for_update()
            )
            if mapping is None:
                session.add(
                    EquipmentMapping(
                        tenant_id=plan.tenant_id,
                        equipment_id=target.equipment_id,
                        tb_device_id=target.tb_device_id,
                        cmms_asset_id=None,
                        cmms_request_digest=request_digest,
                        cmms_request_summary=request_summary,
                        status="RESERVED",
                        enabled=True,
                    )
                )
            elif (
                mapping.equipment_id != target.equipment_id
                or mapping.cmms_request_digest != request_digest
                or mapping.cmms_request_summary != request_summary
            ):
                raise ProvisioningError("EQUIPMENT_RESERVATION_CONFLICT")

    async def activate_target(
        self,
        plan: ProvisioningPlan,
        target: ProvisioningTarget,
        cmms_asset_id: int,
        measurement: dict[str, object],
    ) -> None:
        values = _persisted_measurement(measurement)
        async with self._sessions() as session, session.begin():
            mapping = await session.scalar(
                select(EquipmentMapping)
                .where(
                    EquipmentMapping.tenant_id == plan.tenant_id,
                    EquipmentMapping.tb_device_id == target.tb_device_id,
                )
                .with_for_update()
            )
            if mapping is None or mapping.equipment_id != target.equipment_id:
                raise ProvisioningError("EQUIPMENT_RESERVATION_MISSING")
            if mapping.cmms_asset_id not in (None, cmms_asset_id):
                raise ProvisioningError("CMMS_ASSET_CONFLICT")
            binding = await session.scalar(
                select(MeasurementBinding)
                .where(
                    MeasurementBinding.tenant_id == plan.tenant_id,
                    MeasurementBinding.equipment_id == target.equipment_id,
                    MeasurementBinding.meas_code == values["meas_code"],
                )
                .with_for_update()
            )
            if binding is None:
                session.add(
                    MeasurementBinding(
                        tenant_id=plan.tenant_id,
                        equipment_id=target.equipment_id,
                        risk_threshold=Decimal(str(values["risk_threshold"])),
                        **{key: value for key, value in values.items() if key != "risk_threshold"},
                    )
                )
            elif _measurement_from_row(binding) != values:
                raise ProvisioningError("MEASUREMENT_BINDING_CONFLICT")
            mapping.cmms_asset_id = cmms_asset_id
            mapping.status = "ACTIVE"
            mapping.enabled = True
            session.add(
                AuditEvent(
                    tenant_id=plan.tenant_id,
                    actor_id=plan.actor,
                    action="PILOT_TARGET_PROVISIONED",
                    target_type="TB_DEVICE",
                    target_id=str(target.tb_device_id),
                    correlation_id=uuid5(plan.tenant_id, plan.plan_hash),
                    result_code="ACTIVE",
                    result_summary={
                        "equipment_id": str(target.equipment_id),
                        "cmms_asset_id": cmms_asset_id,
                    },
                )
            )

    async def finish_apply(
        self,
        plan: ProvisioningPlan,
        actor: UUID,
        results: dict[str, dict[str, object]],
        now: Any,
    ) -> None:
        key = (plan.tenant_id, plan.plan_hash)
        session = self._apply_sessions.get(key)
        if session is None:
            raise ProvisioningError("PROVISIONING_APPLY_NOT_STARTED")
        try:
            async with session.begin():
                row = await session.scalar(
                    select(ProvisioningPlanRow)
                    .where(
                        ProvisioningPlanRow.tenant_id == plan.tenant_id,
                        ProvisioningPlanRow.plan_hash == plan.plan_hash,
                    )
                    .with_for_update()
                )
                if row is None or row.applied_at is not None:
                    raise ProvisioningError("PROVISIONING_PLAN_NOT_APPLICABLE")
                row.applied_at = now
                row.target_results = results
                row.terminal_result = "ACTIVE"
                session.add(
                    AuditEvent(
                        tenant_id=plan.tenant_id,
                        actor_id=actor,
                        action="PILOT_PROVISIONING_COMPLETED",
                        target_type="PROVISIONING_PLAN",
                        target_id=plan.plan_hash,
                        correlation_id=uuid5(plan.tenant_id, plan.plan_hash),
                        result_code="ACTIVE",
                        result_summary={
                            "plan_hash": plan.plan_hash,
                            "target_count": len(results),
                        },
                    )
                )
            plan.applied_at = now
            plan.target_results = results
            plan.terminal_result = "ACTIVE"
        finally:
            self._apply_sessions.pop(key, None)
            await self._unlock_and_close(key, session)

    async def read_receipt(self, tenant_id: UUID, plan_hash: str) -> ProvisioningPlan | None:
        async with self._sessions() as session:
            row = await session.scalar(
                select(ProvisioningPlanRow).where(
                    ProvisioningPlanRow.tenant_id == tenant_id,
                    ProvisioningPlanRow.plan_hash == plan_hash,
                    ProvisioningPlanRow.terminal_result == "ACTIVE",
                )
            )
            return None if row is None else _plan_from_row(row)

    @staticmethod
    async def _unlock_and_close(key: tuple[UUID, str], session: AsyncSession) -> None:
        try:
            await session.execute(
                text("SELECT pg_advisory_unlock(hashtext(:tenant_id), hashtext(:plan_hash))"),
                {"tenant_id": str(key[0]), "plan_hash": key[1]},
            )
            await session.commit()
        finally:
            await session.close()
