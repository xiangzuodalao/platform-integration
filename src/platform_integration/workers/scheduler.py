from __future__ import annotations

from datetime import UTC, datetime


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
        for binding in bindings:
            await self._store.create_slot(
                tenant_id=binding.tenant_id,
                equipment_id=binding.equipment_id,
                meas_code=binding.meas_code,
                scheduled_at=slot,
            )
        return len(bindings)
