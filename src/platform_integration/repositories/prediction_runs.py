from datetime import datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import exists, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from platform_integration.models.prediction import PredictionRun


LEASE_DURATION = timedelta(minutes=10)
MAX_ATTEMPTS = 3
CLAIMABLE_STATUSES = ("PENDING", "FAILED", "FAILED_STALE")
SLOT_DURATION = timedelta(minutes=15)


class PredictionRunRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_slot(
        self,
        *,
        tenant_id: UUID,
        equipment_id: UUID,
        meas_code: str,
        scheduled_at: datetime,
    ) -> PredictionRun:
        run_id = uuid4()
        correlation_id = uuid4()
        statement = (
            insert(PredictionRun)
            .values(
                run_id=run_id,
                tenant_id=tenant_id,
                equipment_id=equipment_id,
                meas_code=meas_code,
                scheduled_at=scheduled_at,
                correlation_id=correlation_id,
                status="PENDING",
                attempt=0,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    PredictionRun.tenant_id,
                    PredictionRun.equipment_id,
                    PredictionRun.meas_code,
                    PredictionRun.scheduled_at,
                ]
            )
            .returning(PredictionRun)
        )
        created = (await self._session.scalars(statement)).one_or_none()
        if created is not None:
            return created
        existing = await self._session.scalar(
            select(PredictionRun).where(
                PredictionRun.tenant_id == tenant_id,
                PredictionRun.equipment_id == equipment_id,
                PredictionRun.meas_code == meas_code,
                PredictionRun.scheduled_at == scheduled_at,
            )
        )
        if existing is None:
            raise RuntimeError("prediction slot conflict did not resolve to an existing row")
        return existing

    async def claim(
        self,
        run_id: UUID,
        owner: str,
        now: datetime,
    ) -> PredictionRun | None:
        run = await self._session.scalar(
            select(PredictionRun)
            .where(
                PredictionRun.run_id == run_id,
                PredictionRun.status.in_(CLAIMABLE_STATUSES),
                PredictionRun.attempt < MAX_ATTEMPTS,
            )
            .with_for_update(skip_locked=True)
        )
        if run is None:
            return None
        run.status = "RUNNING"
        run.lease_owner = owner
        run.lease_expires_at = now + LEASE_DURATION
        run.heartbeat_at = now
        run.attempt += 1
        run.error_code = None
        run.completed_at = None
        await self._session.flush()
        return run

    async def claim_next(
        self,
        owner: str,
        now: datetime,
        *,
        tenant_id: UUID,
    ) -> PredictionRun | None:
        run = await self._session.scalar(
            select(PredictionRun)
            .where(
                PredictionRun.status.in_(CLAIMABLE_STATUSES),
                PredictionRun.tenant_id == tenant_id,
                PredictionRun.attempt < MAX_ATTEMPTS,
                PredictionRun.scheduled_at <= now,
                PredictionRun.scheduled_at > now - SLOT_DURATION,
            )
            .order_by(PredictionRun.scheduled_at, PredictionRun.run_id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if run is None:
            return None
        run.status = "RUNNING"
        run.lease_owner = owner
        run.lease_expires_at = now + LEASE_DURATION
        run.heartbeat_at = now
        run.attempt += 1
        run.error_code = None
        run.completed_at = None
        await self._session.flush()
        return run

    async def has_eligible(self, now: datetime, *, tenant_id: UUID) -> bool:
        return bool(
            await self._session.scalar(
                select(
                    exists().where(
                        PredictionRun.status.in_(CLAIMABLE_STATUSES),
                        PredictionRun.tenant_id == tenant_id,
                        PredictionRun.attempt < MAX_ATTEMPTS,
                        PredictionRun.scheduled_at <= now,
                        PredictionRun.scheduled_at > now - SLOT_DURATION,
                    )
                )
            )
        )

    async def heartbeat(self, run_id: UUID, owner: str, now: datetime) -> bool:
        refreshed_id = await self._session.scalar(
            update(PredictionRun)
            .where(
                PredictionRun.run_id == run_id,
                PredictionRun.status == "RUNNING",
                PredictionRun.lease_owner == owner,
                PredictionRun.lease_expires_at > now,
            )
            .values(
                heartbeat_at=now,
                lease_expires_at=now + LEASE_DURATION,
            )
            .returning(PredictionRun.run_id)
        )
        return refreshed_id is not None

    async def reconcile_stale(self, now: datetime, *, tenant_id: UUID) -> list[UUID]:
        result = await self._session.scalars(
            update(PredictionRun)
            .where(
                PredictionRun.status == "RUNNING",
                PredictionRun.tenant_id == tenant_id,
                PredictionRun.lease_expires_at <= now,
            )
            .values(
                status="FAILED_STALE",
                lease_owner=None,
                lease_expires_at=None,
                error_code="LEASE_EXPIRED",
                completed_at=now,
            )
            .returning(PredictionRun.run_id)
        )
        return sorted(result.all(), key=str)

    async def mark_failed(
        self,
        run_id: UUID,
        owner: str,
        now: datetime,
        *,
        code: str,
        retryable: bool,
    ) -> bool:
        values: dict[str, object] = {
            "status": "FAILED",
            "lease_owner": None,
            "lease_expires_at": None,
            "heartbeat_at": now,
            "completed_at": now,
            "error_code": code,
        }
        if not retryable:
            values["attempt"] = MAX_ATTEMPTS
        changed = await self._session.scalar(
            update(PredictionRun)
            .where(
                PredictionRun.run_id == run_id,
                PredictionRun.status == "RUNNING",
                PredictionRun.lease_owner == owner,
            )
            .values(**values)
            .returning(PredictionRun.run_id)
        )
        return changed is not None

    async def mark_skipped(
        self,
        run_id: UUID,
        owner: str,
        now: datetime,
        *,
        code: str,
        quality_summary: dict[str, int],
    ) -> bool:
        changed = await self._session.scalar(
            update(PredictionRun)
            .where(
                PredictionRun.run_id == run_id,
                PredictionRun.status == "RUNNING",
                PredictionRun.lease_owner == owner,
            )
            .values(
                status="SKIPPED_DATA_QUALITY",
                lease_owner=None,
                lease_expires_at=None,
                heartbeat_at=now,
                completed_at=now,
                error_code=code,
                quality_summary=quality_summary,
                attempt=MAX_ATTEMPTS,
            )
            .returning(PredictionRun.run_id)
        )
        return changed is not None
