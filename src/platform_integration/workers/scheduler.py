from __future__ import annotations

from datetime import UTC, datetime


PILOT_BINDING_COUNT = 20


class SchedulerBatchError(ValueError):
    pass


def floor_slot(now: datetime, interval_minutes: int = 15) -> datetime:
    if now.utcoffset() is None:
        raise ValueError("timezone-aware datetime required")
    if type(interval_minutes) is not int or interval_minutes <= 0:
        raise ValueError("positive interval required")
    utc = now.astimezone(UTC)
    minute = utc.minute - utc.minute % interval_minutes
    return utc.replace(minute=minute, second=0, microsecond=0)


class Scheduler:
    def __init__(self, *, store: object, active_bindings) -> None:
        self._store = store
        self._active_bindings = active_bindings

    async def schedule(self, slot: datetime) -> int:
        bindings = self._active_bindings()
        if hasattr(bindings, "__await__"):
            bindings = await bindings
        bindings = list(bindings)
        identities = {
            (binding.tenant_id, binding.equipment_id, binding.meas_code) for binding in bindings
        }
        if len(bindings) != PILOT_BINDING_COUNT or len(identities) != PILOT_BINDING_COUNT:
            raise SchedulerBatchError("SCHEDULER_BINDING_BATCH_INVALID")
        await self._store.create_slot_batch(bindings, scheduled_at=slot)
        return len(bindings)
