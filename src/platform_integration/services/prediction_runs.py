from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from platform_integration.models.bindings import (
    EquipmentMapping,
    MeasurementBinding,
    TenantBinding,
)
from platform_integration.models.prediction import PredictionRun, RiskEvaluationState
from platform_integration.repositories.prediction_runs import PredictionRunRepository
from platform_integration.services.risk import RiskState, advance_risk_state


@dataclass(frozen=True)
class PredictionTarget:
    tenant_id: UUID
    equipment_id: UUID
    tb_device_id: UUID
    meas_code: str
    telemetry_key: str
    unit: str
    sampling_frequency: str
    request_window_points: int
    horizon_points: int
    value_scale: int
    model_profile_id: str
    model_info_id: str
    preprocessing_version: str
    policy_version: str
    risk_direction: str
    risk_threshold: Decimal
    scheduled_at: datetime
    tb_credential_ref: str
    pdm_credential_ref: str


@dataclass(frozen=True)
class ClaimedPrediction:
    run_id: UUID
    tenant_id: UUID
    equipment_id: UUID
    meas_code: str
    scheduled_at: datetime
    correlation_id: UUID
    attempt: int
    lease_owner: str
    binding: PredictionTarget


class ShadowExecutionStore:
    def __init__(
        self,
        *,
        sessions,
        tenant_id: UUID,
        closed_loop_enabled: bool = False,
        pilot_work_order_equipment_id: UUID | None = None,
    ) -> None:
        self._sessions = sessions
        self._tenant_id = tenant_id
        self._closed_loop_enabled = closed_loop_enabled
        self._pilot_work_order_equipment_id = pilot_work_order_equipment_id

    async def active_bindings(self) -> list[PredictionTarget]:
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(MeasurementBinding, EquipmentMapping, TenantBinding)
                    .join(
                        EquipmentMapping,
                        (EquipmentMapping.tenant_id == MeasurementBinding.tenant_id)
                        & (EquipmentMapping.equipment_id == MeasurementBinding.equipment_id),
                    )
                    .join(
                        TenantBinding,
                        TenantBinding.tenant_id == MeasurementBinding.tenant_id,
                    )
                    .where(
                        MeasurementBinding.tenant_id == self._tenant_id,
                        MeasurementBinding.enabled.is_(True),
                        EquipmentMapping.enabled.is_(True),
                        EquipmentMapping.status == "ACTIVE",
                        TenantBinding.enabled.is_(True),
                    )
                    .order_by(MeasurementBinding.equipment_id, MeasurementBinding.meas_code)
                )
            ).all()
        return [
            PredictionTarget(
                tenant_id=measurement.tenant_id,
                equipment_id=measurement.equipment_id,
                tb_device_id=equipment.tb_device_id,
                meas_code=measurement.meas_code,
                telemetry_key=measurement.telemetry_key,
                unit=measurement.unit,
                sampling_frequency=measurement.sampling_frequency,
                request_window_points=measurement.request_window_points,
                horizon_points=measurement.horizon_points,
                value_scale=measurement.value_scale,
                model_profile_id=measurement.model_profile_id,
                model_info_id=measurement.model_info_id,
                preprocessing_version=measurement.preprocessing_version,
                policy_version=measurement.policy_version,
                risk_direction=measurement.risk_direction,
                risk_threshold=measurement.risk_threshold,
                scheduled_at=datetime.min.replace(tzinfo=UTC),
                tb_credential_ref=tenant.tb_credential_ref,
                pdm_credential_ref=tenant.pdm_credential_ref,
            )
            for measurement, equipment, tenant in rows
        ]

    async def create_slot(
        self,
        *,
        tenant_id: UUID,
        equipment_id: UUID,
        meas_code: str,
        scheduled_at: datetime,
    ) -> PredictionRun:
        if tenant_id != self._tenant_id:
            raise ValueError("TENANT_SCOPE_MISMATCH")
        async with self._sessions() as session, session.begin():
            return await PredictionRunRepository(session).create_slot(
                tenant_id=tenant_id,
                equipment_id=equipment_id,
                meas_code=meas_code,
                scheduled_at=scheduled_at,
            )

    async def create_slot_batch(
        self,
        bindings: list[PredictionTarget],
        *,
        scheduled_at: datetime,
    ) -> list[PredictionRun]:
        identities = {
            (binding.tenant_id, binding.equipment_id, binding.meas_code) for binding in bindings
        }
        if len(bindings) != 20 or len(identities) != 20:
            raise ValueError("SCHEDULER_BINDING_BATCH_INVALID")
        if any(binding.tenant_id != self._tenant_id for binding in bindings):
            raise ValueError("TENANT_SCOPE_MISMATCH")
        async with self._sessions() as session, session.begin():
            repository = PredictionRunRepository(session)
            return [
                await repository.create_slot(
                    tenant_id=binding.tenant_id,
                    equipment_id=binding.equipment_id,
                    meas_code=binding.meas_code,
                    scheduled_at=scheduled_at,
                )
                for binding in bindings
            ]

    async def claim_next(self, owner: str, now: datetime) -> ClaimedPrediction | None:
        async with self._sessions() as session, session.begin():
            while True:
                run = await PredictionRunRepository(session).claim_next(
                    owner,
                    now,
                    tenant_id=self._tenant_id,
                )
                if run is None:
                    return None
                joined = (
                    await session.execute(
                        select(MeasurementBinding, EquipmentMapping, TenantBinding)
                        .join(
                            EquipmentMapping,
                            (EquipmentMapping.tenant_id == MeasurementBinding.tenant_id)
                            & (EquipmentMapping.equipment_id == MeasurementBinding.equipment_id),
                        )
                        .join(
                            TenantBinding,
                            TenantBinding.tenant_id == MeasurementBinding.tenant_id,
                        )
                        .where(
                            MeasurementBinding.tenant_id == run.tenant_id,
                            MeasurementBinding.equipment_id == run.equipment_id,
                            MeasurementBinding.meas_code == run.meas_code,
                            MeasurementBinding.enabled.is_(True),
                            EquipmentMapping.enabled.is_(True),
                            EquipmentMapping.status == "ACTIVE",
                            TenantBinding.enabled.is_(True),
                        )
                    )
                ).one_or_none()
                if joined is not None:
                    break
                await PredictionRunRepository(session).mark_failed(
                    run.run_id,
                    owner,
                    now,
                    code="PREDICTION_BINDING_INACTIVE",
                    retryable=False,
                )
                await self._reset_risk_counters_for_run(session, run)
            measurement, equipment, tenant = joined
            binding = PredictionTarget(
                tenant_id=measurement.tenant_id,
                equipment_id=measurement.equipment_id,
                tb_device_id=equipment.tb_device_id,
                meas_code=measurement.meas_code,
                telemetry_key=measurement.telemetry_key,
                unit=measurement.unit,
                sampling_frequency=measurement.sampling_frequency,
                request_window_points=measurement.request_window_points,
                horizon_points=measurement.horizon_points,
                value_scale=measurement.value_scale,
                model_profile_id=measurement.model_profile_id,
                model_info_id=measurement.model_info_id,
                preprocessing_version=measurement.preprocessing_version,
                policy_version=measurement.policy_version,
                risk_direction=measurement.risk_direction,
                risk_threshold=measurement.risk_threshold,
                scheduled_at=run.scheduled_at,
                tb_credential_ref=tenant.tb_credential_ref,
                pdm_credential_ref=tenant.pdm_credential_ref,
            )
            return ClaimedPrediction(
                run_id=run.run_id,
                tenant_id=run.tenant_id,
                equipment_id=run.equipment_id,
                meas_code=run.meas_code,
                scheduled_at=run.scheduled_at,
                correlation_id=run.correlation_id,
                attempt=run.attempt,
                lease_owner=owner,
                binding=binding,
            )

    async def heartbeat(self, run_id: UUID, owner: str, now: datetime) -> bool:
        async with self._sessions() as session, session.begin():
            return await PredictionRunRepository(session).heartbeat(run_id, owner, now)

    async def reconcile_stale(self, now: datetime) -> list[UUID]:
        async with self._sessions() as session, session.begin():
            run_ids = await PredictionRunRepository(session).reconcile_stale(
                now,
                tenant_id=self._tenant_id,
            )
            for run_id in run_ids:
                run = await session.get(PredictionRun, run_id)
                if run is None:
                    continue
                state = await session.scalar(
                    select(RiskEvaluationState)
                    .where(
                        RiskEvaluationState.tenant_id == run.tenant_id,
                        RiskEvaluationState.equipment_id == run.equipment_id,
                        RiskEvaluationState.meas_code == run.meas_code,
                    )
                    .with_for_update()
                )
                if state is None or not await self._is_newest_run(session, state, run):
                    continue
                state.consecutive_risk_count = 0
                state.consecutive_healthy_count = 0
                state.last_prediction_run_id = run.run_id
                state.version += 1
            return run_ids

    async def has_eligible(self, now: datetime) -> bool:
        async with self._sessions() as session:
            return await PredictionRunRepository(session).has_eligible(
                now,
                tenant_id=self._tenant_id,
            )

    async def fail(
        self,
        claim: ClaimedPrediction,
        code: str,
        *,
        transient: bool,
        now: datetime,
    ) -> None:
        async with self._sessions() as session, session.begin():
            changed = await PredictionRunRepository(session).mark_failed(
                claim.run_id,
                claim.lease_owner,
                now,
                code=code,
                retryable=transient,
            )
            if not changed:
                raise RuntimeError("PREDICTION_LEASE_LOST")
            await self._reset_risk_counters(session, claim)

    async def pin_request(self, claim, prepared, *, now: datetime) -> bool:
        quality_summary = self._bounded_quality_summary(prepared.quality_summary)
        async with self._sessions() as session, session.begin():
            run = await session.scalar(
                select(PredictionRun)
                .where(
                    PredictionRun.run_id == claim.run_id,
                    PredictionRun.status == "RUNNING",
                    PredictionRun.lease_owner == claim.lease_owner,
                )
                .with_for_update()
            )
            if run is None:
                raise RuntimeError("PREDICTION_LEASE_LOST")
            if run.request_digest is None:
                run.request_digest = prepared.request.request_digest
                run.model_profile_id = claim.binding.model_profile_id
                run.model_info_id = claim.binding.model_info_id
                run.preprocessing_version = claim.binding.preprocessing_version
                run.policy_version = claim.binding.policy_version
                run.quality_summary = quality_summary
                return True
            if self._snapshot_matches(run, claim, prepared, quality_summary):
                return True
            changed = await PredictionRunRepository(session).mark_failed(
                claim.run_id,
                claim.lease_owner,
                now,
                code="PREDICTION_REQUEST_DRIFT",
                retryable=False,
            )
            if not changed:
                raise RuntimeError("PREDICTION_LEASE_LOST")
            await self._reset_risk_counters(session, claim)
            return False

    async def skip(
        self,
        claim: ClaimedPrediction,
        code: str,
        summary: dict[str, int],
        now: datetime,
    ) -> None:
        async with self._sessions() as session, session.begin():
            run = await session.scalar(
                select(PredictionRun)
                .where(
                    PredictionRun.run_id == claim.run_id,
                    PredictionRun.status == "RUNNING",
                    PredictionRun.lease_owner == claim.lease_owner,
                )
                .with_for_update()
            )
            if run is None:
                raise RuntimeError("PREDICTION_LEASE_LOST")
            repository = PredictionRunRepository(session)
            if run.request_digest is None:
                changed = await repository.mark_skipped(
                    claim.run_id,
                    claim.lease_owner,
                    now,
                    code=code,
                    quality_summary=summary,
                )
            else:
                changed = await repository.mark_failed(
                    claim.run_id,
                    claim.lease_owner,
                    now,
                    code="PREDICTION_REQUEST_DRIFT",
                    retryable=False,
                )
            if not changed:
                raise RuntimeError("PREDICTION_LEASE_LOST")
            await self._reset_risk_counters(session, claim)

    async def succeed(self, claim, prepared, response, risk) -> None:
        async with self._sessions() as session, session.begin():
            run = await session.scalar(
                select(PredictionRun)
                .where(
                    PredictionRun.run_id == claim.run_id,
                    PredictionRun.status == "RUNNING",
                    PredictionRun.lease_owner == claim.lease_owner,
                )
                .with_for_update()
            )
            if run is None:
                raise RuntimeError("PREDICTION_LEASE_LOST")
            quality_summary = self._bounded_quality_summary(prepared.quality_summary)
            if not self._snapshot_matches(run, claim, prepared, quality_summary):
                changed = await PredictionRunRepository(session).mark_failed(
                    claim.run_id,
                    claim.lease_owner,
                    datetime.now(run.scheduled_at.tzinfo),
                    code="PREDICTION_REQUEST_DRIFT",
                    retryable=False,
                )
                if not changed:
                    raise RuntimeError("PREDICTION_LEASE_LOST")
                await self._reset_risk_counters(session, claim)
                return
            run.input_digest = response.input_digest
            run.model_artifact_sha256 = response.model_artifact_sha256
            run.forecast_min = risk.forecast_min
            run.forecast_max = risk.forecast_max
            run.forecast_mean = risk.forecast_mean
            run.threshold_crossing_count = risk.threshold_crossing_count
            run.status = "SUCCEEDED"
            run.error_code = None
            run.completed_at = datetime.now(run.scheduled_at.tzinfo)
            run.lease_owner = None
            run.lease_expires_at = None
            state = await self._locked_state(session, claim)
            if not await self._is_newest_run(session, state, run):
                return
            current = RiskState(
                consecutive_risk_count=state.consecutive_risk_count,
                consecutive_healthy_count=state.consecutive_healthy_count,
                active=state.internal_active,
            )
            next_state = advance_risk_state(current, risky=risk.risky, successful=True)
            was_active = state.internal_active
            state.consecutive_risk_count = next_state.consecutive_risk_count
            state.consecutive_healthy_count = next_state.consecutive_healthy_count
            state.internal_active = next_state.active
            state.last_prediction_run_id = run.run_id
            state.version += 1
            if self._closed_loop_enabled:
                from platform_integration.services.closed_loop import ClosedLoopTransitionService

                await ClosedLoopTransitionService(
                    pilot_work_order_equipment_id=self._pilot_work_order_equipment_id
                ).apply(
                    session,
                    run=run,
                    state=state,
                    binding=claim.binding,
                    risk=risk,
                    was_active=was_active,
                    now=run.completed_at,
                )

    @staticmethod
    def _bounded_quality_summary(summary: object) -> dict[str, int]:
        keys = {
            "distinct_bucket_count",
            "missing_bucket_count",
            "raw_record_count",
        }
        if type(summary) is not dict or set(summary) != keys:
            raise ValueError("QUALITY_SUMMARY_INVALID")
        result: dict[str, int] = {}
        for key in sorted(keys):
            value = summary[key]
            if type(value) is not int or value < 0 or value > 10_000:
                raise ValueError("QUALITY_SUMMARY_INVALID")
            result[key] = value
        return result

    @staticmethod
    def _snapshot_matches(
        run: PredictionRun,
        claim,
        prepared,
        quality_summary: dict[str, int],
    ) -> bool:
        return (
            run.request_digest == prepared.request.request_digest
            and run.model_profile_id == claim.binding.model_profile_id
            and run.model_info_id == claim.binding.model_info_id
            and run.preprocessing_version == claim.binding.preprocessing_version
            and run.policy_version == claim.binding.policy_version
            and run.quality_summary == quality_summary
        )

    async def _reset_risk_counters(self, session, claim) -> None:
        run = await session.get(PredictionRun, claim.run_id)
        if run is None:
            return
        await self._reset_risk_counters_for_run(
            session,
            run,
            policy_version=claim.binding.policy_version,
        )

    async def _reset_risk_counters_for_run(
        self,
        session,
        run: PredictionRun,
        *,
        policy_version: str | None = None,
    ) -> None:
        if policy_version is not None:
            await session.execute(
                insert(RiskEvaluationState)
                .values(
                    tenant_id=run.tenant_id,
                    equipment_id=run.equipment_id,
                    meas_code=run.meas_code,
                    policy_version=policy_version,
                    consecutive_risk_count=0,
                    consecutive_healthy_count=0,
                    internal_active=False,
                    alarm_aggregate_version=0,
                    version=0,
                )
                .on_conflict_do_nothing(
                    index_elements=[
                        RiskEvaluationState.tenant_id,
                        RiskEvaluationState.equipment_id,
                        RiskEvaluationState.meas_code,
                    ]
                )
            )
        state = await session.scalar(
            select(RiskEvaluationState)
            .where(
                RiskEvaluationState.tenant_id == run.tenant_id,
                RiskEvaluationState.equipment_id == run.equipment_id,
                RiskEvaluationState.meas_code == run.meas_code,
            )
            .with_for_update()
        )
        if state is None or not await self._is_newest_run(session, state, run):
            return
        state.consecutive_risk_count = 0
        state.consecutive_healthy_count = 0
        state.last_prediction_run_id = run.run_id
        state.version += 1

    @staticmethod
    async def _is_newest_run(session, state, run: PredictionRun) -> bool:
        if state.last_prediction_run_id is None:
            return True
        previous = await session.get(PredictionRun, state.last_prediction_run_id)
        return (
            previous is None
            or previous.run_id == run.run_id
            or previous.scheduled_at < run.scheduled_at
        )

    @staticmethod
    async def _locked_state(session, claim) -> RiskEvaluationState:
        await session.execute(
            insert(RiskEvaluationState)
            .values(
                tenant_id=claim.tenant_id,
                equipment_id=claim.equipment_id,
                meas_code=claim.meas_code,
                policy_version=claim.binding.policy_version,
                consecutive_risk_count=0,
                consecutive_healthy_count=0,
                internal_active=False,
                alarm_aggregate_version=0,
                version=0,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    RiskEvaluationState.tenant_id,
                    RiskEvaluationState.equipment_id,
                    RiskEvaluationState.meas_code,
                ]
            )
        )
        state = await session.scalar(
            select(RiskEvaluationState)
            .where(
                RiskEvaluationState.tenant_id == claim.tenant_id,
                RiskEvaluationState.equipment_id == claim.equipment_id,
                RiskEvaluationState.meas_code == claim.meas_code,
            )
            .with_for_update()
        )
        if state is None:
            raise RuntimeError("RISK_STATE_UPSERT_FAILED")
        return state
