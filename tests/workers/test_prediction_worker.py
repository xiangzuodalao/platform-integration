import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest


SLOT = datetime(2026, 7, 30, 6, 0, tzinfo=UTC)
RUN_ID = UUID("00000000-0000-4000-8000-000000000501")
QUALITY_SUMMARY = {
    "distinct_bucket_count": 66,
    "missing_bucket_count": 0,
    "raw_record_count": 66,
}


class QueueStore:
    def __init__(self, claims):
        self.claims = list(claims)
        self.heartbeats = []
        self.successes = []
        self.failures = []
        self.skips = []
        self.reconciliations = []
        self.pin_requests = []

    async def reconcile_stale(self, now):
        self.reconciliations.append(now)
        return []

    async def claim_next(self, owner, now):
        del owner, now
        return self.claims.pop(0) if self.claims else None

    async def heartbeat(self, run_id, owner, now):
        self.heartbeats.append((run_id, owner, now))
        return True

    async def succeed(self, claim, prepared, response, risk):
        self.successes.append((claim, prepared, response, risk))

    async def pin_request(self, claim, prepared, *, now):
        self.pin_requests.append((claim, prepared, now))
        return True

    async def fail(self, claim, code, *, transient, now):
        self.failures.append((claim, code, transient, now))

    async def skip(self, claim, code, summary, now):
        self.skips.append((claim, code, summary, now))

    async def has_eligible(self, now):
        del now
        return bool(self.claims)


def claim(attempt=1):
    return SimpleNamespace(
        run_id=RUN_ID,
        attempt=attempt,
        scheduled_at=SLOT,
        correlation_id=UUID("00000000-0000-4000-8000-000000000502"),
        binding=SimpleNamespace(
            scheduled_at=SLOT,
            request_window_points=66,
            tb_device_id=UUID("00000000-0000-4000-8000-000000000503"),
            telemetry_key="vibration",
            unit="mm/s",
            value_scale=2,
            tb_credential_ref="TB_TEST_CREDENTIAL",
            pdm_credential_ref="PDM_TEST_CREDENTIAL",
        ),
    )


async def test_worker_refreshes_owned_ten_minute_lease_every_thirty_seconds():
    """A long prediction without heartbeat can be concurrently reclaimed."""
    from platform_integration.workers.prediction import PredictionWorker

    store = QueueStore([claim()])
    release = asyncio.Event()

    class SlowProcessor:
        async def process(self, _claim):
            await release.wait()

    ticks = iter(
        [
            SLOT,
            SLOT,
            SLOT + timedelta(seconds=30),
            SLOT + timedelta(seconds=60),
            SLOT + timedelta(seconds=60),
        ]
    )
    sleeps = 0

    async def sleep(_seconds):
        nonlocal sleeps
        sleeps += 1
        if sleeps == 3:
            release.set()
        await asyncio.sleep(0)

    worker = PredictionWorker(
        store=store,
        processor=SlowProcessor(),
        owner="worker-a",
        clock=lambda: next(ticks),
        sleep=sleep,
    )
    await worker.run_once()

    assert [item[2] for item in store.heartbeats] == [
        SLOT + timedelta(seconds=30),
        SLOT + timedelta(seconds=60),
    ]


async def test_fast_processor_is_not_delayed_by_the_thirty_second_heartbeat_wait():
    """Awaiting an ordinary sleep task after completion adds 30 seconds to every run."""
    from platform_integration.workers.prediction import PredictionWorker

    class Processor:
        async def process(self, _claim):
            await asyncio.sleep(0.01)

    worker = PredictionWorker(
        store=QueueStore([claim()]),
        processor=Processor(),
        owner="worker-fast",
        clock=lambda: SLOT,
    )

    assert await asyncio.wait_for(worker.run_once(), timeout=0.2) == 1


async def test_stop_during_owned_run_finishes_it_without_claiming_the_next_run():
    """SIGTERM must not abandon the lease or begin another run after the current one."""
    from platform_integration.workers.prediction import PredictionWorker

    stop_requested = asyncio.Event()
    terminalized = []

    class Processor:
        async def process(self, owned):
            stop_requested.set()
            await asyncio.sleep(0)
            terminalized.append(owned.run_id)

    store = QueueStore([claim(), claim()])
    worker = PredictionWorker(
        store=store,
        processor=Processor(),
        owner="worker-signal",
        clock=lambda: SLOT,
    )

    assert await worker.run_once(stop_requested=stop_requested) == 1
    assert terminalized == [RUN_ID]
    assert len(store.claims) == 1


async def test_worker_once_drains_twenty_claims_and_honours_hard_1000_cap():
    """Returning after one row or silently passing a capped backlog breaks batch evidence."""
    from platform_integration.workers.prediction import ClaimLimitExceeded, PredictionWorker

    class Processor:
        async def process(self, _claim):
            return None

    twenty = QueueStore([claim() for _ in range(20)])
    worker = PredictionWorker(
        store=twenty,
        processor=Processor(),
        owner="worker-a",
        clock=lambda: SLOT,
        sleep=lambda _: asyncio.sleep(0),
    )
    assert await worker.run_once() == 20
    assert twenty.reconciliations == [SLOT]

    capped = QueueStore([claim() for _ in range(1001)])
    worker = PredictionWorker(
        store=capped,
        processor=Processor(),
        owner="worker-a",
        clock=lambda: SLOT,
        sleep=lambda _: asyncio.sleep(0),
    )
    with pytest.raises(ClaimLimitExceeded, match="PREDICTION_CLAIM_LIMIT_EXCEEDED"):
        await worker.run_once()


async def test_quality_failure_skips_without_calling_pdm_or_any_alarm_mutation():
    """Calling PDM or an Alarm endpoint after a local gate failure violates shadow isolation."""
    from platform_integration.services.prediction_requests import DataQualityError
    from platform_integration.workers.prediction import PredictionProcessor

    calls = []

    class Builder:
        def prepare(self, *_):
            raise DataQualityError("MISSING_RATIO_EXCEEDED", {"missing_bucket_count": 7})

    class Pdm:
        async def predict(self, _):
            calls.append("pdm")

    class Tb:
        async def historical_telemetry(self, *_args, **_kwargs):
            return ()

        def __getattr__(self, name):
            if "alarm" in name.lower():
                calls.append(name)
            raise AttributeError(name)

    store = QueueStore([])
    processor = PredictionProcessor(
        store=store,
        thingsboard=Tb(),
        pdm=Pdm(),
        request_builder=Builder(),
        risk_evaluator=SimpleNamespace(),
        runtime_tb_credential_ref="TB_TEST_CREDENTIAL",
        runtime_pdm_credential_ref="PDM_TEST_CREDENTIAL",
        clock=lambda: SLOT,
    )
    await processor.process(claim())

    assert calls == []
    assert store.skips[0][1] == "MISSING_RATIO_EXCEEDED"


async def test_transient_pdm_failure_is_bounded_by_attempt_three_and_slot_deadline():
    """Retrying a terminal attempt or a previous slot can overwrite newer risk state."""
    from platform_integration.workers.prediction import PredictionProcessor

    class Builder:
        def prepare(self, *_):
            return SimpleNamespace(request=object(), quality_summary=QUALITY_SUMMARY)

    class Pdm:
        def __init__(self, code="PDM_UNAVAILABLE"):
            self.code = code

        async def predict(self, _):
            from platform_integration.clients.pdm import PdmClientError

            raise PdmClientError(self.code)

    class Tb:
        async def historical_telemetry(self, *_args, **_kwargs):
            return ()

    for attempt, now, transient in (
        (1, SLOT + timedelta(minutes=1), True),
        (3, SLOT + timedelta(minutes=1), False),
        (1, SLOT + timedelta(minutes=15), False),
    ):
        store = QueueStore([])
        processor = PredictionProcessor(
            store=store,
            thingsboard=Tb(),
            pdm=Pdm(),
            request_builder=Builder(),
            risk_evaluator=SimpleNamespace(),
            runtime_tb_credential_ref="TB_TEST_CREDENTIAL",
            runtime_pdm_credential_ref="PDM_TEST_CREDENTIAL",
            clock=lambda now=now: now,
        )
        await processor.process(claim(attempt))
        assert store.failures[0][2] is transient

    store = QueueStore([])
    processor = PredictionProcessor(
        store=store,
        thingsboard=Tb(),
        pdm=Pdm("PDM_REQUEST_FAILED"),
        request_builder=Builder(),
        risk_evaluator=SimpleNamespace(),
        runtime_tb_credential_ref="TB_TEST_CREDENTIAL",
        runtime_pdm_credential_ref="PDM_TEST_CREDENTIAL",
        clock=lambda: SLOT + timedelta(minutes=1),
    )
    await processor.process(claim(1))
    assert store.failures[0][2] is False


async def test_thingsboard_unavailable_retries_without_calling_pdm():
    """Leaving a telemetry fetch failure RUNNING until lease expiry delays bounded recovery."""
    from platform_integration.clients.thingsboard import ThingsBoardClientError
    from platform_integration.workers.prediction import PredictionProcessor

    pdm_calls = []

    class Tb:
        async def historical_telemetry(self, *_args, **_kwargs):
            raise ThingsBoardClientError("THINGSBOARD_UNAVAILABLE")

    class Pdm:
        async def predict(self, _request):
            pdm_calls.append("called")

    store = QueueStore([])
    processor = PredictionProcessor(
        store=store,
        thingsboard=Tb(),
        pdm=Pdm(),
        request_builder=SimpleNamespace(),
        risk_evaluator=SimpleNamespace(),
        runtime_tb_credential_ref="TB_TEST_CREDENTIAL",
        runtime_pdm_credential_ref="PDM_TEST_CREDENTIAL",
        clock=lambda: SLOT + timedelta(minutes=1),
    )

    await processor.process(claim(1))

    assert pdm_calls == []
    assert store.failures[0][1:3] == ("THINGSBOARD_UNAVAILABLE", True)


async def test_invalid_forecast_is_terminalized_without_retry():
    """An invalid aligned/unit forecast must not leave its run RUNNING until lease expiry."""
    from platform_integration.workers.prediction import PredictionProcessor

    class Tb:
        async def historical_telemetry(self, *_args, **_kwargs):
            return ()

    class Builder:
        def prepare(self, *_args):
            return SimpleNamespace(request=object(), quality_summary=QUALITY_SUMMARY)

    class Pdm:
        async def predict(self, _request):
            return SimpleNamespace(forecast=[])

    class Risk:
        def evaluate(self, *_args):
            raise ValueError("provider detail must be redacted")

    store = QueueStore([])
    processor = PredictionProcessor(
        store=store,
        thingsboard=Tb(),
        pdm=Pdm(),
        request_builder=Builder(),
        risk_evaluator=Risk(),
        runtime_tb_credential_ref="TB_TEST_CREDENTIAL",
        runtime_pdm_credential_ref="PDM_TEST_CREDENTIAL",
        clock=lambda: SLOT + timedelta(minutes=1),
    )

    await processor.process(claim())

    assert store.failures[0][1:3] == ("PDM_INVALID_FORECAST", False)


async def test_claim_credential_refs_must_match_runtime_before_external_calls():
    """A stale claim must not route tenant work through different runtime credentials."""
    from platform_integration.workers.prediction import PredictionProcessor

    calls = []

    class Tb:
        async def historical_telemetry(self, *_args, **_kwargs):
            calls.append("tb")
            return ()

    class Pdm:
        async def predict(self, _request):
            calls.append("pdm")

    owned = claim()
    owned.binding.tb_credential_ref = "TB_DIFFERENT_CREDENTIAL"
    store = QueueStore([])
    processor = PredictionProcessor(
        store=store,
        thingsboard=Tb(),
        pdm=Pdm(),
        request_builder=SimpleNamespace(),
        risk_evaluator=SimpleNamespace(),
        runtime_tb_credential_ref="TB_TEST_CREDENTIAL",
        runtime_pdm_credential_ref="PDM_TEST_CREDENTIAL",
        clock=lambda: SLOT,
    )

    await processor.process(owned)

    assert calls == []
    assert store.failures[0][1:3] == (
        "PREDICTION_CREDENTIAL_REF_MISMATCH",
        False,
    )


async def test_request_is_pinned_before_pdm_and_drift_prevents_the_call():
    """PDM must never observe an unpinned or drifted retry request."""
    from platform_integration.workers.prediction import PredictionProcessor

    events = []

    class Store(QueueStore):
        def __init__(self, pin_result):
            super().__init__([])
            self.pin_result = pin_result

        async def pin_request(self, owned, prepared, *, now):
            await super().pin_request(owned, prepared, now=now)
            events.append("pin")
            return self.pin_result

    class Tb:
        async def historical_telemetry(self, *_args, **_kwargs):
            return ()

    class Builder:
        def prepare(self, *_args):
            return SimpleNamespace(request=object(), quality_summary=QUALITY_SUMMARY)

    class Pdm:
        async def predict(self, _request):
            events.append("pdm")
            return SimpleNamespace(forecast=())

    class Risk:
        def evaluate(self, *_args):
            return SimpleNamespace()

    for pin_result, expected in ((True, ["pin", "pdm"]), (False, ["pin"])):
        events.clear()
        store = Store(pin_result)
        processor = PredictionProcessor(
            store=store,
            thingsboard=Tb(),
            pdm=Pdm(),
            request_builder=Builder(),
            risk_evaluator=Risk(),
            runtime_tb_credential_ref="TB_TEST_CREDENTIAL",
            runtime_pdm_credential_ref="PDM_TEST_CREDENTIAL",
            clock=lambda: SLOT,
        )

        await processor.process(claim())

        assert events == expected
        assert len(store.successes) == int(pin_result)
