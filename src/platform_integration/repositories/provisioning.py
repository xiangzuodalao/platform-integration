from __future__ import annotations

from contextlib import suppress
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid5

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

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
    canonical_plan_hash,
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
        apply_actor=row.apply_actor_id,
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
        self._apply_connections: dict[UUID, AsyncConnection] = {}

    async def save_plan(self, snapshot: ValidatedTenantBinding, plan: ProvisioningPlan) -> None:
        async with self._sessions() as session, session.begin():
            tenant_available = await session.scalar(
                text("SELECT pg_try_advisory_xact_lock(hashtext(:tenant_id))"),
                {"tenant_id": str(plan.tenant_id)},
            )
            if tenant_available is not True:
                raise ProvisioningError("PROVISIONING_APPLY_CONCURRENT")
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
                select(ProvisioningPlanRow)
                .where(
                    ProvisioningPlanRow.tenant_id == plan.tenant_id,
                    ProvisioningPlanRow.plan_hash == plan.plan_hash,
                )
                .with_for_update()
            )
            if duplicate is not None:
                canonical_matches = (
                    canonical_plan_hash(duplicate.canonical_plan) == duplicate.plan_hash
                    and duplicate.canonical_plan == plan.canonical
                    and duplicate.tenant_snapshot == plan.tenant_snapshot
                    and duplicate.tb_tenant_id == snapshot.tb_tenant_id
                    and duplicate.cmms_company_id == snapshot.cmms_company_id
                )
                renewable = (
                    duplicate.applied_at is None
                    and duplicate.apply_actor_id is None
                    and duplicate.terminal_result is None
                    and duplicate.expires_at <= plan.created_at
                    and canonical_matches
                )
                if not renewable:
                    raise ProvisioningError("PROVISIONING_PLAN_ALREADY_EXISTS")
                duplicate.actor_id = plan.actor
                duplicate.created_at = plan.created_at
                duplicate.expires_at = plan.expires_at
                duplicate.target_results = None
                return
            session.add(
                ProvisioningPlanRow(
                    tenant_id=plan.tenant_id,
                    plan_hash=plan.plan_hash,
                    canonical_plan=plan.canonical,
                    tenant_snapshot=plan.tenant_snapshot,
                    tb_tenant_id=snapshot.tb_tenant_id,
                    cmms_company_id=snapshot.cmms_company_id,
                    actor_id=plan.actor,
                    apply_actor_id=None,
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
    ) -> dict[UUID, int | None]:
        async with self._sessions() as session:
            mappings = (
                await session.scalars(
                    select(EquipmentMapping).where(EquipmentMapping.tenant_id == plan.tenant_id)
                )
            ).all()
            bindings = (
                await session.scalars(
                    select(MeasurementBinding).where(MeasurementBinding.tenant_id == plan.tenant_id)
                )
            ).all()

        expected = {
            target.tb_device_id: (target, digest, summary)
            for target, digest, summary in reservations
        }
        expected_equipment = {target.equipment_id: target for target, _, _ in reservations}
        persisted_asset_ids: dict[UUID, int | None] = {}
        mapping_by_equipment: dict[UUID, EquipmentMapping] = {}
        for mapping in mappings:
            item = expected.get(mapping.tb_device_id)
            if item is None:
                raise ProvisioningError("PROVISIONING_PERSISTED_STATE_CONFLICT")
            target, digest, summary = item
            common_matches = (
                mapping.equipment_id == target.equipment_id
                and mapping.cmms_request_digest == digest
                and mapping.cmms_request_summary == summary
                and mapping.enabled is True
            )
            state_matches = (mapping.status == "RESERVED" and mapping.cmms_asset_id is None) or (
                mapping.status == "ACTIVE"
                and type(mapping.cmms_asset_id) is int
                and mapping.cmms_asset_id > 0
            )
            if not common_matches or not state_matches:
                raise ProvisioningError("PROVISIONING_PERSISTED_STATE_CONFLICT")
            mapping_by_equipment[mapping.equipment_id] = mapping
            persisted_asset_ids[mapping.tb_device_id] = mapping.cmms_asset_id

        binding_counts: dict[UUID, int] = {}
        for binding in bindings:
            target = expected_equipment.get(binding.equipment_id)
            mapping = mapping_by_equipment.get(binding.equipment_id)
            if (
                target is None
                or mapping is None
                or mapping.status != "ACTIVE"
                or _measurement_from_row(binding)
                != _persisted_measurement(target.measurement_binding)
            ):
                raise ProvisioningError("PROVISIONING_PERSISTED_STATE_CONFLICT")
            binding_counts[binding.equipment_id] = binding_counts.get(binding.equipment_id, 0) + 1
        for equipment_id, mapping in mapping_by_equipment.items():
            expected_count = 1 if mapping.status == "ACTIVE" else 0
            if binding_counts.get(equipment_id, 0) != expected_count:
                raise ProvisioningError("PROVISIONING_PERSISTED_STATE_CONFLICT")
        return persisted_asset_ids

    async def start_apply(self, plan: ProvisioningPlan, actor: UUID, now: Any) -> None:
        tenant_id = plan.tenant_id
        if tenant_id in self._apply_connections:
            raise ProvisioningError("PROVISIONING_APPLY_CONCURRENT")
        engine = self._sessions.kw["bind"]
        connection = await engine.connect()
        lock_acquired = False
        try:
            acquired = await connection.scalar(
                text("SELECT pg_try_advisory_lock(hashtext(:tenant_id))"),
                {"tenant_id": str(tenant_id)},
            )
            await connection.commit()
            if acquired is not True:
                raise ProvisioningError("PROVISIONING_APPLY_CONCURRENT")
            lock_acquired = True
            session = AsyncSession(bind=connection, expire_on_commit=False)
            try:
                row = await session.scalar(
                    select(ProvisioningPlanRow).where(
                        ProvisioningPlanRow.tenant_id == tenant_id,
                        ProvisioningPlanRow.plan_hash == plan.plan_hash,
                    )
                )
                await session.commit()
            finally:
                await session.close()
            if (
                row is None
                or row.applied_at is not None
                or row.terminal_result is not None
                or now >= row.expires_at
            ):
                raise ProvisioningError("PROVISIONING_PLAN_NOT_APPLICABLE")
        except BaseException:
            if lock_acquired:
                await self._unlock_and_close(tenant_id, connection)
            else:
                with suppress(Exception):
                    await connection.close()
            raise
        self._apply_connections[tenant_id] = connection

    async def abort_apply(self, plan: ProvisioningPlan) -> None:
        connection = self._apply_connections.pop(plan.tenant_id, None)
        if connection is not None:
            await self._unlock_and_close(plan.tenant_id, connection)

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
        actor: UUID,
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
                    actor_id=actor,
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
        tenant_id = plan.tenant_id
        connection = self._apply_connections.get(tenant_id)
        if connection is None:
            raise ProvisioningError("PROVISIONING_APPLY_NOT_STARTED")
        session = AsyncSession(bind=connection, expire_on_commit=False)
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
                row.apply_actor_id = actor
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
            plan.apply_actor = actor
            plan.target_results = results
            plan.terminal_result = "ACTIVE"
        finally:
            await session.close()
            self._apply_connections.pop(tenant_id, None)
            await self._unlock_and_close(tenant_id, connection)

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
    async def _unlock_and_close(tenant_id: UUID, connection: AsyncConnection) -> None:
        try:
            with suppress(Exception):
                await connection.rollback()
                await connection.execute(
                    text("SELECT pg_advisory_unlock(hashtext(:tenant_id))"),
                    {"tenant_id": str(tenant_id)},
                )
                await connection.commit()
        finally:
            await connection.close()
