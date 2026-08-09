from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID

import pytest

from platform_integration.services.closed_loop import risk_key
from platform_integration.services.closed_loop_acceptance import (
    ClosedLoopAcceptanceError,
    ClosedLoopAcceptanceSnapshot,
    ClosedLoopAcceptanceVerifier,
    OUTBOX_STATUSES,
)


TENANT_ID = UUID("00000000-0000-4000-8000-000000000001")
TB_TENANT_ID = UUID("00000000-0000-4000-8000-000000000002")
EQUIPMENT_ID = UUID("00000000-0000-4000-8000-000000000003")
DEVICE_ID = UUID("00000000-0000-4000-8000-000000000004")
ALERT_ID = UUID("00000000-0000-4000-8000-000000000005")
ALARM_ID = UUID("00000000-0000-4000-8000-000000000006")
CORRELATION_ID = UUID("00000000-0000-4000-8000-000000000007")


def snapshot(**changes) -> ClosedLoopAcceptanceSnapshot:
    key = risk_key(EQUIPMENT_ID, "vibration_rms")
    details = {
        "alert_id": str(ALERT_ID),
        "risk_key": key,
        "equipment_id": str(EQUIPMENT_ID),
        "meas_code": "vibration_rms",
        "maintenance_alert_version": 4,
        "risk_state": "ACTIVE",
        "maintenance_state": "WORK_ORDER_OPEN",
    }
    request_body = {
        "asset": {"id": 701},
        "external_source": "PDM_FORECAST",
        "external_ref": str(ALERT_ID),
        "correlation_id": str(CORRELATION_ID),
        "equipment_id": str(EQUIPMENT_ID),
        "tb_alarm_id": str(ALARM_ID),
        "model_profile_id": "pilot-profile",
        "model_info_id": "pilot-model",
        "policy_version": "pilot-policy-v1",
    }
    base = ClosedLoopAcceptanceSnapshot(
        tenant_id=TENANT_ID,
        tenant_alias="ifactory-pilot",
        tb_tenant_id=TB_TENANT_ID,
        cmms_company_id=7001,
        tb_credential_ref="TB_PILOT_CREDENTIAL",
        cmms_credential_ref="CMMS_PILOT_CREDENTIAL",
        equipment_id=EQUIPMENT_ID,
        tb_device_id=DEVICE_ID,
        cmms_asset_id=701,
        meas_code="vibration_rms",
        state_alert_id=ALERT_ID,
        state_internal_active=True,
        consecutive_healthy_count=0,
        state_alarm_aggregate_version=6,
        alert_id=ALERT_ID,
        risk_key=key,
        episode=1,
        correlation_id=CORRELATION_ID,
        model_profile_id="pilot-profile",
        model_info_id="pilot-model",
        policy_version="pilot-policy-v1",
        alert_risk_state="ACTIVE",
        alert_maintenance_state="WORK_ORDER_OPEN",
        alert_version=4,
        alarm_id=ALARM_ID,
        alarm_desired_active=True,
        alarm_type="PDM_FORECAST_RISK",
        alarm_severity="WARNING",
        alarm_aggregate_version=6,
        alarm_details=details,
        external_ref=ALERT_ID,
        work_order_request_body=request_body,
        cmms_work_order_id=42,
        cmms_status="OPEN",
        cmms_event_version=0,
        work_order_maintenance_state="WORK_ORDER_OPEN",
        outbox={status: (4 if status == "DELIVERED" else 0) for status in OUTBOX_STATUSES},
    )
    return replace(base, **changes)


def alarm_for(current: ClosedLoopAcceptanceSnapshot, *, acknowledged=True, cleared=False):
    status = f"{'CLEARED' if cleared else 'ACTIVE'}_{'ACK' if acknowledged else 'UNACK'}"
    return SimpleNamespace(
        alarm_id=current.alarm_id,
        tenant_id=current.tb_tenant_id,
        device_id=current.tb_device_id,
        alarm_type=current.alarm_type,
        severity=current.alarm_severity,
        status=status,
        details=current.alarm_details,
        update_fields={"acknowledged": acknowledged, "cleared": cleared},
    )


def work_order_for(current: ClosedLoopAcceptanceSnapshot):
    request = current.work_order_request_body
    return SimpleNamespace(
        id=current.cmms_work_order_id,
        status=current.cmms_status,
        event_version=current.cmms_event_version,
        external_source="PDM_FORECAST",
        external_ref=current.external_ref,
        correlation_id=UUID(request["correlation_id"]),
        equipment_id=UUID(request["equipment_id"]),
        tb_alarm_id=UUID(request["tb_alarm_id"]),
        model_profile_id=request["model_profile_id"],
        model_info_id=request["model_info_id"],
        policy_version=request["policy_version"],
        asset_id=request["asset"]["id"],
    )


class Store:
    def __init__(self, current):
        self.current = current
        self.calls = []

    async def read(self, **arguments):
        self.calls.append(arguments)
        if isinstance(self.current, Exception):
            raise self.current
        return self.current


class ThingsBoard:
    def __init__(self, current, alarm=None):
        self.current = current
        self.alarm = alarm or alarm_for(current)
        self.calls = []

    async def authenticated_tenant_id(self):
        self.calls.append("identity")
        return self.current.tb_tenant_id

    async def alarm_state(self, alarm_id):
        self.calls.append(("alarm", alarm_id))
        return self.alarm


class Cmms:
    def __init__(self, current, work_order=None):
        self.current = current
        self.work_order = work_order or work_order_for(current)
        self.calls = []

    async def authenticated_company_id(self):
        self.calls.append("identity")
        return self.current.cmms_company_id

    async def find_work_order_by_external_ref(self, external_ref):
        self.calls.append(("work-order", external_ref))
        return self.work_order


def verifier(current, *, thingsboard=None, cmms=None):
    return ClosedLoopAcceptanceVerifier(
        store=Store(current),
        thingsboard=thingsboard or ThingsBoard(current),
        cmms=cmms or Cmms(current),
        tenant_id=TENANT_ID,
        tb_tenant_id=TB_TENANT_ID,
        cmms_company_id=7001,
        tb_credential_ref="TB_PILOT_CREDENTIAL",
        cmms_credential_ref="CMMS_PILOT_CREDENTIAL",
        equipment_id=EQUIPMENT_ID,
    )


@pytest.mark.asyncio
async def test_verify_reads_both_stable_apis_and_returns_bounded_secret_free_evidence():
    current = snapshot()
    tb = ThingsBoard(current)
    cmms = Cmms(current)

    evidence = await verifier(current, thingsboard=tb, cmms=cmms).verify(
        tenant_alias="ifactory-pilot",
        expected_stage="ACTIVE",
    )

    assert evidence["status"] == "VERIFIED"
    assert evidence["observed_stage"] == "ACTIVE"
    assert evidence["thingsboard_alarm"] == {
        "id": str(ALARM_ID),
        "device_id": str(DEVICE_ID),
        "type": "PDM_FORECAST_RISK",
        "severity": "WARNING",
        "status": "ACTIVE_ACK",
        "acknowledged": True,
        "cleared": False,
        "details_sha256": evidence["thingsboard_alarm"]["details_sha256"],
        "maintenance_alert_version": 4,
    }
    assert evidence["cmms_work_order"]["external_ref"] == str(ALERT_ID)
    assert tb.calls == ["identity", ("alarm", ALARM_ID)]
    assert cmms.calls == ["identity", ("work-order", ALERT_ID)]
    assert "CREDENTIAL" not in repr(evidence)


@pytest.mark.asyncio
async def test_verify_returns_not_ready_without_touching_providers_when_outbox_is_unsettled():
    current = snapshot(outbox={**snapshot().outbox, "RETRY_WAIT": 1})
    tb = ThingsBoard(current)
    cmms = Cmms(current)

    with pytest.raises(ClosedLoopAcceptanceError) as exc_info:
        await verifier(current, thingsboard=tb, cmms=cmms).verify(
            tenant_alias="ifactory-pilot",
            expected_stage="CONSISTENT",
        )

    assert exc_info.value.code == "CLOSED_LOOP_ACCEPTANCE_NOT_READY"
    assert tb.calls == []
    assert cmms.calls == []


@pytest.mark.asyncio
async def test_verify_rejects_dead_letter_separately_from_transient_not_ready():
    current = snapshot(outbox={**snapshot().outbox, "DEAD_LETTER": 1})

    with pytest.raises(ClosedLoopAcceptanceError) as exc_info:
        await verifier(current).verify(
            tenant_alias="ifactory-pilot",
            expected_stage="CONSISTENT",
        )

    assert exc_info.value.code == "CLOSED_LOOP_ACCEPTANCE_OUTBOX_DEAD_LETTER"


@pytest.mark.asyncio
async def test_complete_stage_allows_alarm_to_remain_active_until_two_healthy_rounds():
    current = snapshot(
        alert_maintenance_state="WORK_ORDER_COMPLETE",
        cmms_status="COMPLETE",
        cmms_event_version=2,
        work_order_maintenance_state="WORK_ORDER_COMPLETE",
        alarm_details={
            **snapshot().alarm_details,
            "maintenance_state": "WORK_ORDER_COMPLETE",
        },
    )

    evidence = await verifier(current).verify(
        tenant_alias="ifactory-pilot",
        expected_stage="COMPLETE",
    )

    assert evidence["observed_stage"] == "COMPLETE"
    assert evidence["thingsboard_alarm"]["cleared"] is False


@pytest.mark.asyncio
async def test_active_stage_does_not_accept_a_later_work_order_milestone():
    current = snapshot(
        alert_maintenance_state="WORK_ORDER_IN_PROGRESS",
        cmms_status="IN_PROGRESS",
        cmms_event_version=1,
        work_order_maintenance_state="WORK_ORDER_IN_PROGRESS",
        alarm_details={
            **snapshot().alarm_details,
            "maintenance_state": "WORK_ORDER_IN_PROGRESS",
        },
    )

    with pytest.raises(ClosedLoopAcceptanceError) as exc_info:
        await verifier(current).verify(
            tenant_alias="ifactory-pilot",
            expected_stage="ACTIVE",
        )

    assert exc_info.value.code == "CLOSED_LOOP_ACCEPTANCE_NOT_READY"


@pytest.mark.asyncio
async def test_cleared_stage_proves_complete_work_order_two_healthy_rounds_and_cleared_alarm():
    cleared_details = {
        **snapshot().alarm_details,
        "risk_state": "CLEARED",
        "maintenance_state": "WORK_ORDER_COMPLETE",
        "maintenance_alert_version": 8,
    }
    current = snapshot(
        state_internal_active=False,
        consecutive_healthy_count=2,
        alert_risk_state="CLEARED",
        alert_maintenance_state="WORK_ORDER_COMPLETE",
        alert_version=8,
        alarm_desired_active=False,
        alarm_details=cleared_details,
        cmms_status="COMPLETE",
        cmms_event_version=2,
        work_order_maintenance_state="WORK_ORDER_COMPLETE",
    )
    tb = ThingsBoard(current, alarm=alarm_for(current, acknowledged=True, cleared=True))

    evidence = await verifier(current, thingsboard=tb).verify(
        tenant_alias="ifactory-pilot",
        expected_stage="CLEARED",
    )

    assert evidence["observed_stage"] == "CLEARED"
    assert evidence["aggregate"]["consecutive_healthy_count"] == 2
    assert evidence["cmms_work_order"]["status"] == "COMPLETE"
    assert evidence["thingsboard_alarm"]["cleared"] is True

    with pytest.raises(ClosedLoopAcceptanceError) as exc_info:
        await verifier(current, thingsboard=tb).verify(
            tenant_alias="ifactory-pilot",
            expected_stage="COMPLETE",
        )
    assert exc_info.value.code == "CLOSED_LOOP_ACCEPTANCE_NOT_READY"


@pytest.mark.asyncio
async def test_cleared_stage_is_not_ready_after_only_one_healthy_round():
    current = snapshot(
        state_internal_active=False,
        consecutive_healthy_count=1,
        alert_risk_state="CLEARED",
        alert_maintenance_state="WORK_ORDER_COMPLETE",
        alarm_desired_active=False,
        alarm_details={
            **snapshot().alarm_details,
            "risk_state": "CLEARED",
            "maintenance_state": "WORK_ORDER_COMPLETE",
        },
        cmms_status="COMPLETE",
        cmms_event_version=2,
        work_order_maintenance_state="WORK_ORDER_COMPLETE",
    )
    tb = ThingsBoard(current, alarm=alarm_for(current, cleared=True))

    with pytest.raises(ClosedLoopAcceptanceError) as exc_info:
        await verifier(current, thingsboard=tb).verify(
            tenant_alias="ifactory-pilot",
            expected_stage="CLEARED",
        )

    assert exc_info.value.code == "CLOSED_LOOP_ACCEPTANCE_NOT_READY"


@pytest.mark.asyncio
async def test_complete_stage_rejects_an_early_manually_cleared_alarm():
    current = snapshot(
        state_internal_active=False,
        alert_risk_state="MANUALLY_CLOSED",
        alert_maintenance_state="WORK_ORDER_COMPLETE",
        alarm_desired_active=False,
        alarm_details={
            **snapshot().alarm_details,
            "risk_state": "MANUALLY_CLOSED",
            "maintenance_state": "WORK_ORDER_COMPLETE",
        },
        cmms_status="COMPLETE",
        cmms_event_version=2,
        work_order_maintenance_state="WORK_ORDER_COMPLETE",
    )
    tb = ThingsBoard(current, alarm=alarm_for(current, cleared=True))

    with pytest.raises(ClosedLoopAcceptanceError) as exc_info:
        await verifier(current, thingsboard=tb).verify(
            tenant_alias="ifactory-pilot",
            expected_stage="COMPLETE",
        )

    assert exc_info.value.code == "CLOSED_LOOP_ACCEPTANCE_NOT_READY"


@pytest.mark.asyncio
async def test_provider_work_order_version_drift_fails_closed():
    current = snapshot()
    drifted = work_order_for(current)
    drifted.event_version = 9

    with pytest.raises(ClosedLoopAcceptanceError) as exc_info:
        await verifier(current, cmms=Cmms(current, work_order=drifted)).verify(
            tenant_alias="ifactory-pilot",
            expected_stage="CONSISTENT",
        )

    assert exc_info.value.code == "CLOSED_LOOP_ACCEPTANCE_WORK_ORDER_MISMATCH"


@pytest.mark.asyncio
async def test_provider_alarm_status_must_exactly_match_ack_and_clear_booleans():
    current = snapshot()
    malformed = alarm_for(current)
    malformed.status = "ALMOST_ACTIVE_ACK"

    with pytest.raises(ClosedLoopAcceptanceError) as exc_info:
        await verifier(current, thingsboard=ThingsBoard(current, alarm=malformed)).verify(
            tenant_alias="ifactory-pilot",
            expected_stage="CONSISTENT",
        )

    assert exc_info.value.code == "CLOSED_LOOP_ACCEPTANCE_ALARM_MISMATCH"


@pytest.mark.asyncio
async def test_verify_rechecks_the_database_after_provider_reads_to_reject_a_race():
    current = snapshot()

    class ChangingStore:
        calls = 0

        async def read(self, **_):
            self.calls += 1
            return current if self.calls == 1 else replace(current, alert_version=5)

    instance = ClosedLoopAcceptanceVerifier(
        store=ChangingStore(),
        thingsboard=ThingsBoard(current),
        cmms=Cmms(current),
        tenant_id=TENANT_ID,
        tb_tenant_id=TB_TENANT_ID,
        cmms_company_id=7001,
        tb_credential_ref="TB_PILOT_CREDENTIAL",
        cmms_credential_ref="CMMS_PILOT_CREDENTIAL",
        equipment_id=EQUIPMENT_ID,
    )

    with pytest.raises(ClosedLoopAcceptanceError) as exc_info:
        await instance.verify(
            tenant_alias="ifactory-pilot",
            expected_stage="CONSISTENT",
        )

    assert exc_info.value.code == "CLOSED_LOOP_ACCEPTANCE_NOT_READY"


@pytest.mark.asyncio
async def test_local_work_order_request_cannot_drift_from_the_alert_identity():
    current = snapshot(
        work_order_request_body={
            **snapshot().work_order_request_body,
            "model_info_id": "another-model",
        }
    )

    with pytest.raises(ClosedLoopAcceptanceError) as exc_info:
        await verifier(current).verify(
            tenant_alias="ifactory-pilot",
            expected_stage="CONSISTENT",
        )

    assert exc_info.value.code == "CLOSED_LOOP_ACCEPTANCE_WORK_ORDER_MISMATCH"


@pytest.mark.asyncio
async def test_no_current_alert_is_explicitly_not_ready_and_secret_free():
    unavailable = ClosedLoopAcceptanceError("CLOSED_LOOP_ACCEPTANCE_NOT_READY")
    store = Store(unavailable)
    secret = "acceptance-secret-canary"
    verifier_instance = ClosedLoopAcceptanceVerifier(
        store=store,
        thingsboard=SimpleNamespace(secret=secret),
        cmms=SimpleNamespace(secret=secret),
        tenant_id=TENANT_ID,
        tb_tenant_id=TB_TENANT_ID,
        cmms_company_id=7001,
        tb_credential_ref="TB_PILOT_CREDENTIAL",
        cmms_credential_ref="CMMS_PILOT_CREDENTIAL",
        equipment_id=EQUIPMENT_ID,
    )

    with pytest.raises(ClosedLoopAcceptanceError) as exc_info:
        await verifier_instance.verify(
            tenant_alias="ifactory-pilot",
            expected_stage="CONSISTENT",
        )

    assert exc_info.value.code == "CLOSED_LOOP_ACCEPTANCE_NOT_READY"
    assert secret not in str(exc_info.value)
