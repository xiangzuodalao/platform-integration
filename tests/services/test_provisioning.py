from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest

from platform_integration.clients.thingsboard import ThingsBoardDevice
from platform_integration.services.provisioning import (
    ProvisioningError,
    ProvisioningService,
    canonical_plan_hash,
)
from platform_integration.services.tenant_bindings import (
    ISOLATED_TENANT_ID,
    PILOT_ALIAS,
    TenantBindingConfig,
    TenantBindingsService,
    expected_pilot_devices,
)


NOW = datetime(2026, 7, 30, 6, 0, tzinfo=UTC)
ACTOR = UUID("00000000-0000-4000-8000-000000000099")
APPLY_ACTOR = UUID("00000000-0000-4000-8000-000000000098")


def devices() -> list[ThingsBoardDevice]:
    return [
        ThingsBoardDevice(
            id=UUID(f"00000000-0000-4000-8000-{index:012d}"),
            name=name,
            device_type=device_type,
        )
        for index, (name, device_type) in enumerate(expected_pilot_devices().items(), start=1)
    ]


class FakeThingsBoard:
    def __init__(self, target_devices=None) -> None:
        self.devices = target_devices or devices()
        self.attributes: dict[UUID, SimpleNamespace] = {}
        self.write_calls: list[tuple[UUID, UUID, int]] = []

    async def authenticated_tenant_id(self):
        return ISOLATED_TENANT_ID

    async def list_devices(self):
        return self.devices

    async def active_pdm_alarm_count(self, _):
        return 0

    async def write_asset_attributes(self, device_id, *, equipment_id, cmms_asset_id):
        self.write_calls.append((device_id, equipment_id, cmms_asset_id))
        self.attributes[device_id] = SimpleNamespace(
            equipment_id=equipment_id,
            cmms_asset_id=cmms_asset_id,
        )

    async def read_asset_attributes(self, device_id):
        return self.attributes.get(
            device_id, SimpleNamespace(equipment_id=None, cmms_asset_id=None)
        )


class FakeCmms:
    def __init__(self) -> None:
        self.assets = {}
        self.create_calls = []

    async def authenticated_company_id(self):
        return 201

    async def total_work_orders(self):
        return 0

    async def find_asset_by_equipment_id(self, equipment_id):
        return self.assets.get(equipment_id)

    async def create_asset(self, request, *, idempotency_key):
        self.create_calls.append((request, idempotency_key))
        asset = SimpleNamespace(id=len(self.assets) + 1, **request.model_dump())
        self.assets[request.equipment_id] = asset
        return asset


class FakeStore:
    def __init__(self) -> None:
        self.plans = {}
        self.events = []
        self.reservations = {}
        self.bindings = {}

    async def save_plan(self, snapshot, plan):
        self.events.append(("save_plan", snapshot))
        self.plans[plan.plan_hash] = plan

    async def load_plan(self, plan_hash):
        self.events.append(("load_plan", plan_hash))
        return self.plans.get(plan_hash)

    async def start_apply(self, plan, actor, now):
        self.events.append(("start_apply", plan.plan_hash, actor, now))

    async def abort_apply(self, plan):
        self.events.append(("abort_apply", plan.plan_hash))

    async def preflight_plan(self, plan, reservations):
        self.events.append(("preflight", plan.plan_hash))
        persisted_assets = {}
        for target, request_digest, request_summary in reservations:
            existing = self.reservations.get(target.tb_device_id)
            proposed = (target.equipment_id, request_digest, request_summary)
            if existing is not None and existing != proposed:
                raise ProvisioningError("EQUIPMENT_RESERVATION_CONFLICT")
            binding = self.bindings.get(target.tb_device_id)
            if binding is not None and binding != target.measurement_binding:
                raise ProvisioningError("MEASUREMENT_BINDING_CONFLICT")
        return persisted_assets

    async def reserve_target(self, plan, target, request_digest, request_summary):
        self.events.append(("reserve", target.tb_device_id, request_digest, request_summary))
        previous = self.reservations.get(target.tb_device_id)
        current = (target.equipment_id, request_digest, request_summary)
        if previous is not None and previous != current:
            raise ProvisioningError("EQUIPMENT_RESERVATION_CONFLICT")
        self.reservations[target.tb_device_id] = current

    async def activate_target(self, plan, target, cmms_asset_id, measurement, actor):
        self.events.append(("activate", target.tb_device_id, cmms_asset_id, measurement, actor))
        key = target.tb_device_id
        previous = self.bindings.get(key)
        if previous is not None and previous != measurement:
            raise ProvisioningError("MEASUREMENT_BINDING_CONFLICT")
        self.bindings[key] = measurement

    async def finish_apply(self, plan, actor, results, now):
        self.events.append(("finish", plan.plan_hash, actor, results, now))
        plan.applied_at = now
        plan.apply_actor = actor
        plan.terminal_result = "ACTIVE"
        plan.target_results = results

    async def read_receipt(self, tenant_id, plan_hash):
        plan = self.plans.get(plan_hash)
        if plan is None or plan.tenant_id != tenant_id or plan.terminal_result != "ACTIVE":
            return None
        return plan


def service(tb=None, cmms=None, store=None, now=lambda: NOW):
    tb = tb or FakeThingsBoard()
    cmms = cmms or FakeCmms()
    store = store or FakeStore()
    config = TenantBindingConfig(
        tenant_id=ISOLATED_TENANT_ID,
        alias=PILOT_ALIAS,
        tb_tenant_id=ISOLATED_TENANT_ID,
        cmms_company_id=201,
        pdm_credential_ref="PDM_PILOT_CREDENTIAL",
        tb_credential_ref="TB_PILOT_CREDENTIAL",
        cmms_credential_ref="CMMS_PILOT_CREDENTIAL",
    )
    bindings = TenantBindingsService(config, tb=tb, cmms=cmms)
    return (
        ProvisioningService(
            tenant_bindings=bindings,
            store=store,
            thingsboard=tb,
            cmms=cmms,
            now=now,
        ),
        tb,
        cmms,
        store,
    )


@pytest.mark.asyncio
async def test_plan_is_canonical_secret_free_exact_and_expires_in_thirty_minutes() -> None:
    """Omitting a target/profile field or persisting a token would make confirmation incomplete."""
    provisioning, _, _, store = service()

    plan = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)

    assert plan.created_at == NOW
    assert plan.expires_at == NOW + timedelta(minutes=30)
    assert len(plan.canonical["targets"]) == 20
    assert [target["tb_device_id"] for target in plan.canonical["targets"]] == sorted(
        target["tb_device_id"] for target in plan.canonical["targets"]
    )
    first = next(
        target for target in plan.canonical["targets"] if target["device_name"] == "LINE-A-CNC-01"
    )
    assert first["asset_name"] == "LINE-A-CNC-01"
    assert first["device_type"] == "CNC"
    assert first["measurement_binding"] == {
        "meas_code": "vibration_rms",
        "telemetry_key": "vibration_rms",
        "unit": "mm/s",
        "sampling_frequency": "1min",
        "request_window_points": 66,
        "context_points": 60,
        "horizon_points": 15,
        "value_scale": 2,
        "model_profile_id": "pilot-cnc-vibration",
        "model_info_id": "pilot-fixture-v1-cnc-vibration",
        "preprocessing_version": "pdm-v2-pilot-1",
        "policy_version": "pilot-threshold-v1",
        "predictor_kind": "repeat-last",
        "risk_direction": "ABOVE",
        "risk_threshold": "5.00",
        "enabled": True,
    }
    persisted = repr(store.plans)
    assert "secret" not in persisted.lower()
    assert "token" not in persisted.lower()

    modified = deepcopy(plan.canonical)
    modified["targets"][0]["asset_name"] = "changed"
    assert canonical_plan_hash(modified) != plan.plan_hash
    modified = deepcopy(plan.canonical)
    modified["targets"][0]["measurement_binding"]["request_window_points"] = 65
    assert canonical_plan_hash(modified) != plan.plan_hash


@pytest.mark.asyncio
async def test_invalid_device_type_or_set_fails_before_any_local_or_external_write() -> None:
    """A display label or missing simulator target must never reach persistence."""
    invalid = devices()
    invalid[0] = ThingsBoardDevice(
        id=invalid[0].id,
        name=invalid[0].name,
        device_type="Injection molding",
    )
    provisioning, tb, cmms, store = service(tb=FakeThingsBoard(invalid))

    with pytest.raises(ProvisioningError, match="PILOT_DEVICE_SET_INVALID"):
        await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)

    assert store.events == []
    assert tb.write_calls == []
    assert cmms.create_calls == []


@pytest.mark.asyncio
async def test_apply_rejects_absent_mismatch_expired_or_applied_plan_without_writes() -> None:
    """Weak confirmation gates could partially apply an unconfirmed or stale plan."""
    provisioning, tb, cmms, store = service()

    with pytest.raises(ProvisioningError, match="PROVISIONING_PLAN_NOT_FOUND"):
        await provisioning.apply("a" * 64, "a" * 64, ACTOR)

    plan = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)
    event_count = len(store.events)
    with pytest.raises(ProvisioningError, match="PROVISIONING_CONFIRMATION_MISMATCH"):
        await provisioning.apply(plan.plan_hash, "b" * 64, ACTOR)
    assert len(store.events) == event_count + 1  # read-only load only

    plan.expires_at = NOW - timedelta(seconds=1)
    with pytest.raises(ProvisioningError, match="PROVISIONING_PLAN_EXPIRED"):
        await provisioning.apply(plan.plan_hash, plan.plan_hash, ACTOR)

    plan.expires_at = NOW + timedelta(minutes=1)
    plan.applied_at = NOW
    plan.terminal_result = "ACTIVE"
    with pytest.raises(ProvisioningError, match="PROVISIONING_PLAN_ALREADY_APPLIED"):
        await provisioning.apply(plan.plan_hash, plan.plan_hash, ACTOR)

    assert tb.write_calls == []
    assert cmms.create_calls == []


@pytest.mark.asyncio
async def test_verify_reads_exact_tenant_hash_receipt_and_requires_all_twenty_targets() -> None:
    """A broad receipt lookup or partial receipt could falsely report successful provisioning."""
    provisioning, _, _, store = service()
    plan = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)
    plan.terminal_result = "ACTIVE"
    plan.target_results = {
        str(target.tb_device_id): {"result_code": "ACTIVE"} for target in plan.targets
    }

    receipt = await provisioning.verify(ISOLATED_TENANT_ID, plan.plan_hash)
    assert receipt.plan_hash == plan.plan_hash

    plan.target_results.pop(next(iter(plan.target_results)))
    with pytest.raises(ProvisioningError, match="PROVISIONING_RECEIPT_INCOMPLETE"):
        await provisioning.verify(ISOLATED_TENANT_ID, plan.plan_hash)
    with pytest.raises(ProvisioningError, match="PROVISIONING_RECEIPT_NOT_FOUND"):
        await provisioning.verify(UUID(int=2), plan.plan_hash)


@pytest.mark.asyncio
async def test_receipt_distinguishes_build_actor_from_confirming_apply_actor() -> None:
    """Returning only the plan builder would lose the identity that confirmed external writes."""
    provisioning, _, _, _ = service()
    plan = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)

    result = await provisioning.apply(plan.plan_hash, plan.plan_hash, APPLY_ACTOR)

    assert result.actor == ACTOR
    assert result.apply_actor == APPLY_ACTOR
