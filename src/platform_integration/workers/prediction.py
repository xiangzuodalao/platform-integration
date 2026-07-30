from __future__ import annotations

import asyncio
from datetime import timedelta

from platform_integration.clients.pdm import PdmClientError
from platform_integration.clients.thingsboard import ThingsBoardClientError
from platform_integration.services.data_quality import INTERVAL_MS
from platform_integration.services.prediction_requests import DataQualityError


HEARTBEAT_SECONDS = 30
CLAIM_LIMIT = 1000
TRANSIENT_PDM_CODES = {"PDM_UNAVAILABLE"}
TRANSIENT_TB_CODES = {"THINGSBOARD_UNAVAILABLE"}


class ClaimLimitExceeded(RuntimeError):
    pass


class PredictionWorker:
    def __init__(self, *, store, processor, owner: str, clock, sleep=asyncio.sleep) -> None:
        self._store = store
        self._processor = processor
        self._owner = owner
        self._clock = clock
        self._sleep = sleep

    async def run_once(self, *, stop_requested: asyncio.Event | None = None) -> int:
        if stop_requested is not None and stop_requested.is_set():
            return 0
        await self._store.reconcile_stale(self._clock())
        claimed = 0
        while claimed < CLAIM_LIMIT:
            if stop_requested is not None and stop_requested.is_set():
                return claimed
            run = await self._store.claim_next(self._owner, self._clock())
            if run is None:
                return claimed
            claimed += 1
            stopped = asyncio.Event()
            heartbeat = asyncio.create_task(self._heartbeat(run.run_id, stopped))
            try:
                await self._processor.process(run)
            finally:
                stopped.set()
                await heartbeat
        if await self._store.has_eligible(self._clock()):
            raise ClaimLimitExceeded("PREDICTION_CLAIM_LIMIT_EXCEEDED")
        return claimed

    async def _heartbeat(self, run_id, stopped: asyncio.Event) -> None:
        while not stopped.is_set():
            sleep_task = asyncio.create_task(self._sleep(HEARTBEAT_SECONDS))
            stop_task = asyncio.create_task(stopped.wait())
            done, pending = await asyncio.wait(
                (sleep_task, stop_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            await asyncio.gather(*done)
            if stop_task in done or stopped.is_set():
                return
            if not await self._store.heartbeat(run_id, self._owner, self._clock()):
                raise RuntimeError("PREDICTION_LEASE_LOST")


class PredictionProcessor:
    def __init__(
        self,
        *,
        store,
        thingsboard,
        pdm,
        request_builder,
        risk_evaluator,
        runtime_tb_credential_ref: str,
        runtime_pdm_credential_ref: str,
        clock,
    ) -> None:
        self._store = store
        self._thingsboard = thingsboard
        self._pdm = pdm
        self._request_builder = request_builder
        self._risk_evaluator = risk_evaluator
        self._runtime_tb_credential_ref = runtime_tb_credential_ref
        self._runtime_pdm_credential_ref = runtime_pdm_credential_ref
        self._clock = clock

    async def process(self, claim) -> None:
        binding = claim.binding
        if (
            binding.tb_credential_ref != self._runtime_tb_credential_ref
            or binding.pdm_credential_ref != self._runtime_pdm_credential_ref
        ):
            await self._store.fail(
                claim,
                "PREDICTION_CREDENTIAL_REF_MISMATCH",
                transient=False,
                now=self._clock(),
            )
            return
        window_end = int(claim.scheduled_at.timestamp() * 1000)
        window_start = window_end - int(binding.request_window_points) * INTERVAL_MS
        try:
            points = await self._thingsboard.historical_telemetry(
                binding.tb_device_id,
                telemetry_key=binding.telemetry_key,
                unit=binding.unit,
                start_ms=window_start,
                end_exclusive_ms=window_end,
            )
        except ThingsBoardClientError as exc:
            now = self._clock()
            transient = (
                exc.code in TRANSIENT_TB_CODES
                and claim.attempt < 3
                and now < claim.scheduled_at + timedelta(minutes=15)
            )
            await self._store.fail(claim, exc.code, transient=transient, now=now)
            return
        try:
            prepared = self._request_builder.prepare(
                binding,
                points,
                claim.correlation_id,
            )
        except DataQualityError as exc:
            await self._store.skip(claim, exc.code, exc.summary, self._clock())
            return
        if not await self._store.pin_request(claim, prepared, now=self._clock()):
            return
        try:
            response = await self._pdm.predict(prepared.request)
        except PdmClientError as exc:
            now = self._clock()
            transient = (
                exc.code in TRANSIENT_PDM_CODES
                and claim.attempt < 3
                and now < claim.scheduled_at + timedelta(minutes=15)
            )
            await self._store.fail(claim, exc.code, transient=transient, now=now)
            return
        try:
            risk = self._risk_evaluator.evaluate(binding, response.forecast)
        except ValueError:
            await self._store.fail(
                claim,
                "PDM_INVALID_FORECAST",
                transient=False,
                now=self._clock(),
            )
            return
        await self._store.succeed(claim, prepared, response, risk)
