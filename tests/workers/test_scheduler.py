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
            self.batch_calls = 0

        async def create_slot_batch(self, batch, *, scheduled_at):
            self.batch_calls += 1
            for binding in batch:
                self.rows.add(
                    (
                        binding.tenant_id,
                        binding.equipment_id,
                        binding.meas_code,
                        scheduled_at,
                    )
                )

    store = Store()
    scheduler = Scheduler(store=store, active_bindings=lambda: bindings)
    slot = datetime(2026, 7, 30, 6, 0, tzinfo=UTC)
    assert await scheduler.schedule(slot) == 20
    assert await scheduler.schedule(slot) == 20
    assert len(store.rows) == 20
    assert store.batch_calls == 2


@pytest.mark.parametrize("case", ["empty", "nineteen", "twenty_one", "duplicate"])
async def test_scheduler_rejects_non_pilot_or_duplicate_batch_before_any_write(case):
    """A partial or ambiguous pilot slot must leave no rows behind."""
    from platform_integration.workers.scheduler import Scheduler, SchedulerBatchError

    tenant_id = UUID("00000000-0000-4000-8000-000000000001")
    bindings = [
        SimpleNamespace(
            tenant_id=tenant_id,
            equipment_id=UUID(f"20000000-0000-4000-8000-{index:012d}"),
            meas_code=f"measurement-{index:02d}",
        )
        for index in range(21)
    ]
    selected = {
        "empty": [],
        "nineteen": bindings[:19],
        "twenty_one": bindings,
        "duplicate": [*bindings[:19], bindings[0]],
    }[case]

    class Store:
        def __init__(self):
            self.batch_calls = 0

        async def create_slot_batch(self, _batch, *, scheduled_at):
            del scheduled_at
            self.batch_calls += 1

    store = Store()
    scheduler = Scheduler(store=store, active_bindings=lambda: selected)

    with pytest.raises(SchedulerBatchError, match="SCHEDULER_BINDING_BATCH_INVALID"):
        await scheduler.schedule(datetime(2026, 7, 30, 6, 0, tzinfo=UTC))

    assert store.batch_calls == 0
