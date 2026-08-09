from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID

import pytest

from platform_integration.services.tenant_bindings import (
    ISOLATED_TENANT_ID,
    PILOT_ALIAS,
    TenantBindingConfig,
    TenantBindingError,
    TenantBindingsService,
    expected_pilot_devices,
)


class FakeThingsBoard:
    tenant_id = UUID("30000000-0000-4000-8000-000000000001")

    def __init__(self, *, alarm_device: UUID | None = None) -> None:
        self.alarm_device = alarm_device
        self.alarm_checks: list[UUID] = []

    async def authenticated_tenant_id(self) -> UUID:
        return self.tenant_id

    async def active_pdm_alarm_count(self, device_id: UUID) -> int:
        self.alarm_checks.append(device_id)
        return 1 if device_id == self.alarm_device else 0


class FakeCmms:
    company_id = 201
    work_orders = 0

    async def authenticated_company_id(self) -> int:
        return self.company_id

    async def total_work_orders(self) -> int:
        return self.work_orders


def config() -> TenantBindingConfig:
    return TenantBindingConfig(
        tenant_id=ISOLATED_TENANT_ID,
        alias=PILOT_ALIAS,
        tb_tenant_id=UUID("30000000-0000-4000-8000-000000000001"),
        cmms_company_id=201,
        pdm_credential_ref="PDM_PILOT_CREDENTIAL",
        tb_credential_ref="TB_PILOT_CREDENTIAL",
        cmms_credential_ref="CMMS_PILOT_CREDENTIAL",
    )


@pytest.mark.asyncio
async def test_readiness_requires_exact_configured_and_authenticated_identities() -> None:
    """Allowing alias or authenticated identity drift could write into a shared tenant."""
    targets = tuple(UUID(int=index) for index in range(1, 21))

    with pytest.raises(TenantBindingError, match="PILOT_ALIAS_TENANT_MISMATCH"):
        TenantBindingsService(
            replace(config(), tenant_id=UUID("00000000-0000-4000-8000-000000000002")),
            tb=FakeThingsBoard(),
            cmms=FakeCmms(),
        )

    tb = FakeThingsBoard()
    tb.tenant_id = UUID("00000000-0000-4000-8000-000000000002")
    service = TenantBindingsService(config(), tb=tb, cmms=FakeCmms())
    with pytest.raises(TenantBindingError, match="THINGSBOARD_TENANT_IDENTITY_MISMATCH"):
        await service.validate_readiness(targets)

    cmms = FakeCmms()
    cmms.company_id = 202
    service = TenantBindingsService(config(), tb=FakeThingsBoard(), cmms=cmms)
    with pytest.raises(TenantBindingError, match="CMMS_COMPANY_IDENTITY_MISMATCH"):
        await service.validate_readiness(targets)


@pytest.mark.asyncio
async def test_readiness_checks_exact_twenty_devices_and_zero_isolation_baseline() -> None:
    """Skipping a device or tolerating any prior alarm/work order breaks pilot isolation."""
    targets = tuple(UUID(int=index) for index in range(1, 21))
    tb = FakeThingsBoard()
    cmms = FakeCmms()
    service = TenantBindingsService(config(), tb=tb, cmms=cmms)

    snapshot = await service.validate_readiness(targets)

    assert snapshot.tb_tenant_id == UUID("30000000-0000-4000-8000-000000000001")
    assert snapshot.cmms_company_id == 201
    assert tb.alarm_checks == list(targets)

    with pytest.raises(TenantBindingError, match="PILOT_TARGET_COUNT_INVALID"):
        await service.validate_readiness(targets[:-1])

    alarm_tb = FakeThingsBoard(alarm_device=targets[-1])
    with pytest.raises(TenantBindingError) as alarm_error:
        await TenantBindingsService(config(), tb=alarm_tb, cmms=cmms).validate_readiness(targets)
    assert alarm_error.value.code == "ISOLATION_BASELINE_NOT_CLEAN"
    assert alarm_error.value.details == {
        "tb_tenant_id": "30000000-0000-4000-8000-000000000001",
        "active_pdm_alarm_count": 1,
        "cmms_company_id": 201,
        "work_order_count": 0,
        "operator_action": "use a clean isolated environment",
    }

    dirty_cmms = FakeCmms()
    dirty_cmms.work_orders = 2
    with pytest.raises(TenantBindingError) as work_order_error:
        await TenantBindingsService(
            config(), tb=FakeThingsBoard(), cmms=dirty_cmms
        ).validate_readiness(targets)
    assert work_order_error.value.details["work_order_count"] == 2
    assert "delete" not in str(work_order_error.value).lower()
    assert "reset" not in str(work_order_error.value).lower()


def test_expected_devices_are_a_strict_case_sensitive_allowlist() -> None:
    """Pilot targets stay exact while unrelated tenant devices remain outside the closed loop."""
    expected = expected_pilot_devices()

    assert len(expected) == 20
    assert expected["LINE-A-CNC-01"] == "CNC"
    assert expected["LINE-A-CNC-02"] == "CNC"
    assert expected["LINE-B-EOL_TESTER-01"] == "EOL_TESTER"
    assert set(expected.values()) == {
        "CNC",
        "INJECTION_MOLDING",
        "ASSEMBLY_ROBOT",
        "TIGHTENING",
        "AIR_COMPRESSOR",
        "EOL_TESTER",
    }

    service = TenantBindingsService(config(), tb=FakeThingsBoard(), cmms=FakeCmms())
    valid = [
        SimpleNamespace(id=UUID(int=index), name=name, device_type=device_type)
        for index, (name, device_type) in enumerate(expected.items(), start=1)
    ]
    assert service.validate_target_devices(valid) == tuple(
        sorted(valid, key=lambda device: str(device.id))
    )

    unrelated = [
        SimpleNamespace(id=UUID(int=101), name="NON-PILOT-CNC", device_type="CNC"),
        SimpleNamespace(id=UUID(int=102), name="测试设备", device_type="default"),
    ]
    assert service.validate_target_devices([*valid, *unrelated]) == tuple(
        sorted(valid, key=lambda device: str(device.id))
    )

    with pytest.raises(TenantBindingError, match="PILOT_DEVICE_SET_INVALID"):
        service.validate_target_devices([*valid[:-1], unrelated[0]])

    duplicate = [*valid, replace_device(valid[0], device_type=valid[0].device_type)]
    duplicate[-1] = SimpleNamespace(
        id=UUID(int=103),
        name=duplicate[-1].name,
        device_type=duplicate[-1].device_type,
    )
    with pytest.raises(TenantBindingError, match="PILOT_DEVICE_SET_INVALID"):
        service.validate_target_devices(duplicate)

    duplicate_target_id = [*valid]
    duplicate_target_id[1] = SimpleNamespace(
        id=duplicate_target_id[0].id,
        name=duplicate_target_id[1].name,
        device_type=duplicate_target_id[1].device_type,
    )
    with pytest.raises(TenantBindingError, match="PILOT_DEVICE_SET_INVALID"):
        service.validate_target_devices(duplicate_target_id)

    unrelated_reused_id = [
        *valid,
        SimpleNamespace(id=valid[0].id, name="NON-PILOT-CNC", device_type="CNC"),
    ]
    with pytest.raises(TenantBindingError, match="PILOT_DEVICE_SET_INVALID"):
        service.validate_target_devices(unrelated_reused_id)

    display_label = [*valid]
    display_label[0] = replace_device(display_label[0], device_type="Injection molding")
    with pytest.raises(TenantBindingError, match="PILOT_DEVICE_SET_INVALID"):
        service.validate_target_devices(display_label)

    lower_case = [*valid]
    lower_case[0] = replace_device(lower_case[0], device_type="cnc")
    with pytest.raises(TenantBindingError, match="PILOT_DEVICE_SET_INVALID"):
        service.validate_target_devices(lower_case)


def replace_device(device, *, device_type: str):
    return SimpleNamespace(id=device.id, name=device.name, device_type=device_type)
