from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import pytest

from platform_integration.clients.cmms import CmmsClientError
from platform_integration.services.closed_loop import risk_key
from platform_integration.services.closed_loop_delivery import (
    AlarmDelivery,
    AlarmDeliveryWorker,
    OutboxClaim,
    StatusPollClaim,
    StatusPollWorker,
    WorkOrderDelivery,
    WorkOrderDeliveryWorker,
)


NOW = datetime(2026, 8, 4, 6, 0, tzinfo=UTC)
TENANT_ID = UUID("00000000-0000-4000-8000-000000000001")
ALERT_ID = UUID("00000000-0000-4000-8000-000000000101")
DEVICE_ID = UUID("00000000-0000-4000-8000-000000000201")
ALARM_ID = UUID("00000000-0000-4000-8000-000000000301")


def claim(event_type: str) -> OutboxClaim:
    return OutboxClaim(
        event_id=UUID("00000000-0000-4000-8000-000000000401"),
        tenant_id=TENANT_ID,
        aggregate_id=ALERT_ID,
        event_type=event_type,
        aggregate_version=1,
        attempt=1,
        owner="worker-1",
    )


def work_order_response():
    return SimpleNamespace(
        id=42,
        status="OPEN",
        event_version=0,
        updated_at=NOW,
        external_source="PDM_FORECAST",
        external_ref=ALERT_ID,
        correlation_id=UUID("00000000-0000-4000-8000-000000000501"),
        equipment_id=UUID("00000000-0000-4000-8000-000000000601"),
        tb_alarm_id=ALARM_ID,
        model_profile_id="pilot-profile",
        model_info_id="pilot-model",
        policy_version="pilot-policy-v1",
        asset_id=701,
    )


def work_order_delivery() -> WorkOrderDelivery:
    return WorkOrderDelivery(
        claim=claim("CMMS_WORK_ORDER_CREATE"),
        alert_id=ALERT_ID,
        credential_ref="CMMS_PILOT_CREDENTIAL",
        cmms_company_id=7001,
        request_body={
            "asset": {"id": 701},
            "external_ref": str(ALERT_ID),
            "correlation_id": "00000000-0000-4000-8000-000000000501",
            "equipment_id": "00000000-0000-4000-8000-000000000601",
            "tb_alarm_id": str(ALARM_ID),
            "model_profile_id": "pilot-profile",
            "model_info_id": "pilot-model",
            "policy_version": "pilot-policy-v1",
        },
    )


@pytest.mark.asyncio
async def test_work_order_worker_always_gets_before_posting_and_recovers_existing_result():
    """A response-loss retry must recover the one external-ref row without another POST."""
    delivery = work_order_delivery()
    calls = []

    class Store:
        async def claim_outbox(self, *_):
            return delivery.claim

        async def work_order_delivery(self, _):
            return delivery

        async def save_created_work_order(self, _, response, __):
            calls.append(("saved", response.id))

        async def retry(self, *_):
            raise AssertionError("unexpected retry")

        async def dead_letter(self, *_):
            raise AssertionError("unexpected dead letter")

    class Client:
        async def authenticated_company_id(self):
            return 7001

        async def find_work_order_by_external_ref(self, external_ref):
            calls.append(("GET", external_ref))
            return work_order_response()

        async def create_work_order(self, *_args, **_kwargs):
            raise AssertionError("POST must not run after recovery")

    worker = WorkOrderDeliveryWorker(
        store=Store(), cmms_factory=lambda _: Client(), owner="worker-1"
    )

    assert await worker.run_once(NOW)
    assert calls == [("GET", ALERT_ID), ("saved", 42)]


@pytest.mark.asyncio
async def test_work_order_worker_does_not_post_when_reconciliation_get_fails():
    """An unavailable identity read must result in zero blind writes."""
    delivery = work_order_delivery()
    calls = []

    class Store:
        async def claim_outbox(self, *_):
            return delivery.claim

        async def work_order_delivery(self, _):
            return delivery

        async def retry(self, _, code, __):
            calls.append(("retry", code))

        async def dead_letter(self, *_):
            raise AssertionError("unexpected dead letter")

    class Client:
        async def authenticated_company_id(self):
            return 7001

        async def find_work_order_by_external_ref(self, _):
            calls.append("GET")
            raise CmmsClientError("CMMS_UNAVAILABLE")

        async def create_work_order(self, *_args, **_kwargs):
            calls.append("POST")

    worker = WorkOrderDeliveryWorker(
        store=Store(), cmms_factory=lambda _: Client(), owner="worker-1"
    )

    assert await worker.run_once(NOW)
    assert calls == ["GET", ("retry", "CMMS_UNAVAILABLE")]


@pytest.mark.asyncio
async def test_alarm_worker_reconciles_by_risk_key_before_upsert():
    """An unknown Alarm write result must converge on its durable risk identity."""
    event = claim("TB_ALARM_SYNC")
    delivery = AlarmDelivery(
        claim=event,
        alert_id=ALERT_ID,
        risk_key=risk_key(DEVICE_ID, "vibration_rms"),
        tb_tenant_id=TENANT_ID,
        device_id=DEVICE_ID,
        alarm_id=None,
        desired_active=True,
        details={
            "alert_id": str(ALERT_ID),
            "risk_key": risk_key(DEVICE_ID, "vibration_rms"),
            "equipment_id": str(DEVICE_ID),
            "meas_code": "vibration_rms",
        },
    )
    calls = []

    class Store:
        async def claim_outbox(self, *_):
            return event

        async def alarm_delivery(self, _):
            return delivery

        async def save_alarm_id(self, _, alarm_id, __):
            calls.append(("saved", alarm_id))

    class ThingsBoard:
        async def authenticated_tenant_id(self):
            return TENANT_ID

        async def find_pdm_alarm(self, device_id, **identity):
            calls.append(("find", device_id, identity))
            return None

        async def upsert_pdm_alarm(self, **kwargs):
            calls.append(("upsert", kwargs["existing_alarm"]))
            return ALARM_ID

    worker = AlarmDeliveryWorker(store=Store(), thingsboard=ThingsBoard(), owner="worker-1")

    assert await worker.run_once(NOW)
    assert calls == [
        (
            "find",
            DEVICE_ID,
            {
                "risk_key": risk_key(DEVICE_ID, "vibration_rms"),
                "alert_id": ALERT_ID,
                "equipment_id": str(DEVICE_ID),
                "meas_code": "vibration_rms",
            },
        ),
        ("upsert", None),
        ("saved", ALARM_ID),
    ]


@pytest.mark.asyncio
async def test_status_poll_bounds_read_retries_and_never_creates():
    """Feedback is read-only and gets at most three attempts in one due cycle."""
    poll = StatusPollClaim(
        work_order_ref_id=UUID("00000000-0000-4000-8000-000000000701"),
        tenant_id=TENANT_ID,
        alert_id=ALERT_ID,
        external_ref=ALERT_ID,
        credential_ref="CMMS_PILOT_CREDENTIAL",
        cmms_company_id=7001,
        request_body=work_order_delivery().request_body,
        owner="status-1",
    )
    calls = []

    class Store:
        async def claim_status_poll(self, *_):
            return poll

        async def fail_status_poll(self, _, code, __):
            calls.append(("failed", code))

    class Client:
        async def authenticated_company_id(self):
            return 7001

        async def find_work_order_by_external_ref(self, _):
            calls.append("GET")
            raise CmmsClientError("CMMS_UNAVAILABLE")

    worker = StatusPollWorker(store=Store(), cmms_factory=lambda _: Client(), owner="status-1")

    assert await worker.run_once(NOW)
    assert calls == ["GET", "GET", "GET", ("failed", "CMMS_UNAVAILABLE")]


@pytest.mark.asyncio
async def test_status_poll_rejects_any_frozen_identity_drift_before_state_apply():
    """Matching only external_ref could import another asset or model's status into the Alarm."""
    poll = StatusPollClaim(
        work_order_ref_id=UUID("00000000-0000-4000-8000-000000000702"),
        tenant_id=TENANT_ID,
        alert_id=ALERT_ID,
        external_ref=ALERT_ID,
        credential_ref="CMMS_PILOT_CREDENTIAL",
        cmms_company_id=7001,
        request_body=work_order_delivery().request_body,
        owner="status-2",
    )
    calls = []
    drifted = work_order_response()
    drifted.model_info_id = "wrong-model"

    class Store:
        async def claim_status_poll(self, *_):
            return poll

        async def apply_status(self, *_):
            calls.append("APPLIED")

        async def fail_status_poll(self, _, code, __):
            calls.append(("failed", code))

    class Client:
        async def authenticated_company_id(self):
            return 7001

        async def find_work_order_by_external_ref(self, _):
            return drifted

    worker = StatusPollWorker(store=Store(), cmms_factory=lambda _: Client(), owner="status-2")

    assert await worker.run_once(NOW)
    assert calls == [("failed", "CMMS_WORK_ORDER_IDENTITY_MISMATCH")]
