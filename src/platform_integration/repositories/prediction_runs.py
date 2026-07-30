from datetime import datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from platform_integration.models.prediction import PredictionRun


LEASE_DURATION = timedelta(minutes=10)
MAX_ATTEMPTS = 3
CLAIMABLE_STATUSES = ("PENDING", "FAILED_STALE")


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

    async def reconcile_stale(self, now: datetime) -> list[UUID]:
        result = await self._session.scalars(
            update(PredictionRun)
            .where(
                PredictionRun.status == "RUNNING",
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
