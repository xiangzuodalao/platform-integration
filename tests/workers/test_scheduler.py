from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest


def test_floor_slot_requires_aware_time_converts_to_utc_and_floors_15_minutes():
    """Accepting naive/local slots can create duplicate runs for one real instant."""
    from platform_integration.workers.scheduler import floor_slot

    local = datetime(2026, 7, 30, 14, 22, 10, tzinfo=timezone(timedelta(hours=8)))
    assert floor_slot(local) == datetime(2026, 7, 30, 6, 15, tzinfo=UTC)
    with pytest.raises(ValueError, match="timezone-aware"):
        floor_slot(datetime(2026, 7, 30, 6, 22))


async def test_repeated_scheduler_calls_create_one_run_per_active_binding_and_slot():
    """Creating a second row on replay breaks idempotent slot scheduling."""
    from platform_integration.workers.scheduler import Scheduler

    bindings = [
        SimpleNamespace(
            tenant_id=UUID("00000000-0000-4000-8000-000000000001"),
            equipment_id=UUID(f"10000000-0000-4000-8000-{index:012d}"),
            meas_code=f"measurement-{index:02d}",
        )
        for index in range(20)
    ]

    class Store:
        def __init__(self):
            self.rows = set()

        async def create_slot(self, *, tenant_id, equipment_id, meas_code, scheduled_at):
            self.rows.add((tenant_id, equipment_id, meas_code, scheduled_at))

    store = Store()
    scheduler = Scheduler(store=store, active_bindings=lambda: bindings)
    slot = datetime(2026, 7, 30, 6, 0, tzinfo=UTC)
    assert await scheduler.schedule(slot) == 20
    assert await scheduler.schedule(slot) == 20
    assert len(store.rows) == 20
