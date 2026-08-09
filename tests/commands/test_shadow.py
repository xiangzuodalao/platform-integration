from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import pytest

from platform_integration.config import Settings


FIXED_SLOT = datetime(2026, 7, 29, 1, 15, tzinfo=UTC)
WALL_CLOCK = datetime(2026, 7, 30, 6, 0, tzinfo=UTC)


def test_run_prediction_worker_forwards_fixed_time_to_async_command(monkeypatch):
    """Discarding the parsed time in the sync wrapper restores the wall-clock bug."""
    from platform_integration.commands import shadow

    calls = []

    async def record_worker(*, once, now):
        calls.append((once, now))

    monkeypatch.setattr(shadow, "_prediction_worker", record_worker)

    shadow.run_prediction_worker(once=True, now=FIXED_SLOT)

    assert calls == [(True, FIXED_SLOT)]


async def test_prediction_worker_command_forwards_fixed_time_to_runtime(monkeypatch):
    """Discarding time before runtime construction makes the fixed slot ineligible."""
    from platform_integration.commands import shadow

    calls = []

    class Worker:
        async def run_once(self):
            calls.append("run")
            return 0

    class Runtime:
        worker = Worker()

        async def close(self):
            calls.append("close")

    sessions = object()
    settings = object()
    monkeypatch.setattr(shadow, "_database_settings", lambda: (settings, sessions))

    def runtime_factory(received_settings, received_sessions, *, now):
        calls.append((received_settings, received_sessions, now))
        return Runtime()

    monkeypatch.setattr(shadow, "_prediction_runtime", runtime_factory)
    monkeypatch.setattr(shadow, "_install_stop_handlers", lambda _stopped: None)

    await shadow._prediction_worker(once=True, now=FIXED_SLOT)

    assert calls == [
        (settings, sessions, FIXED_SLOT),
        "run",
        "close",
    ]


@pytest.mark.parametrize(
    ("injected_now", "expected"),
    [
        (FIXED_SLOT, FIXED_SLOT),
        (None, WALL_CLOCK),
    ],
)
async def test_prediction_runtime_uses_one_injected_or_wall_clock_for_worker_and_processor(
    monkeypatch,
    injected_now,
    expected,
):
    """A wall clock in either runtime consumer can reject or mis-time the fixed slot."""
    from platform_integration.commands import shadow

    class ControlledDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            assert tz is UTC
            return WALL_CLOCK

    class Bind:
        async def dispose(self):
            return None

    for name in (
        "ALL_PROXY",
        "HTTPS_PROXY",
        "HTTP_PROXY",
        "all_proxy",
        "https_proxy",
        "http_proxy",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(shadow, "datetime", ControlledDateTime)
    runtime = shadow._prediction_runtime(
        Settings(
            tenant_id=UUID("00000000-0000-4000-8000-000000000001"),
            tb_base_url="http://thingsboard.invalid",
            pdm_base_url="http://pdm.invalid",
            tb_credential_ref="TB_TEST_CREDENTIAL",
            pdm_credential_ref="PDM_TEST_CREDENTIAL",
        ),
        SimpleNamespace(kw={"bind": Bind()}),
        now=injected_now,
    )
    worker_times = []
    processor_times = []

    async def reconcile_stale(now):
        worker_times.append(("reconcile", now))
        return []

    async def claim_next(_owner, now):
        worker_times.append(("claim", now))
        return None

    async def record_failure(_claim, _code, *, transient, now):
        assert transient is False
        processor_times.append(now)

    runtime.worker._store.reconcile_stale = reconcile_stale
    runtime.worker._store.claim_next = claim_next
    runtime.worker._processor._store.fail = record_failure

    try:
        assert await runtime.worker.run_once() == 0
        await runtime.worker._processor.process(
            SimpleNamespace(
                binding=SimpleNamespace(
                    tb_credential_ref="TB_OTHER_CREDENTIAL",
                    pdm_credential_ref="PDM_TEST_CREDENTIAL",
                )
            )
        )
    finally:
        await runtime.close()

    assert worker_times == [
        ("reconcile", expected),
        ("claim", expected),
    ]
    assert processor_times == [expected]
