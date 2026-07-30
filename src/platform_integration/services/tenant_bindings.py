from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID


PILOT_ALIAS = "ifactory-pilot"
ISOLATED_TENANT_ID = UUID("00000000-0000-4000-8000-000000000001")


def expected_pilot_devices() -> dict[str, str]:
    result: dict[str, str] = {}
    counts = (
        ("CNC", 2),
        ("INJECTION_MOLDING", 2),
        ("ASSEMBLY_ROBOT", 2),
        ("TIGHTENING", 2),
        ("AIR_COMPRESSOR", 1),
        ("EOL_TESTER", 1),
    )
    for line in ("LINE-A", "LINE-B"):
        for device_type, count in counts:
            for index in range(1, count + 1):
                result[f"{line}-{device_type}-{index:02d}"] = device_type
    return result


class ThingsBoardReadiness(Protocol):
    async def authenticated_tenant_id(self) -> UUID: ...

    async def active_pdm_alarm_count(self, device_id: UUID) -> int: ...


class CmmsReadiness(Protocol):
    async def authenticated_company_id(self) -> int: ...

    async def total_work_orders(self) -> int: ...


class TargetDevice(Protocol):
    id: UUID
    name: str
    device_type: str


class TenantBindingError(RuntimeError):
    def __init__(self, code: str, details: dict[str, object] | None = None) -> None:
        self.code = code
        self.details = details
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class TenantBindingConfig:
    tenant_id: UUID
    alias: str
    tb_tenant_id: UUID
    cmms_company_id: int
    pdm_credential_ref: str
    tb_credential_ref: str
    cmms_credential_ref: str
    cmms_webhook_secret_ref: str | None = None


@dataclass(frozen=True, slots=True)
class ValidatedTenantBinding:
    tenant_id: UUID
    alias: str
    tb_tenant_id: UUID
    cmms_company_id: int
    pdm_credential_ref: str
    tb_credential_ref: str
    cmms_credential_ref: str
    cmms_webhook_secret_ref: str | None


class TenantBindingsService:
    def __init__(
        self,
        config: TenantBindingConfig,
        *,
        tb: ThingsBoardReadiness,
        cmms: CmmsReadiness,
    ) -> None:
        if (
            config.alias != PILOT_ALIAS
            or config.tenant_id != ISOLATED_TENANT_ID
            or config.tb_tenant_id != ISOLATED_TENANT_ID
        ):
            raise TenantBindingError("PILOT_ALIAS_TENANT_MISMATCH")
        if type(config.cmms_company_id) is not int or config.cmms_company_id <= 0:
            raise TenantBindingError("CMMS_COMPANY_ID_INVALID")
        self.config = config
        self._tb = tb
        self._cmms = cmms

    def validate_target_devices(self, devices: Sequence[TargetDevice]) -> tuple[TargetDevice, ...]:
        expected = expected_pilot_devices()
        actual = {device.name: device.device_type for device in devices}
        unique_ids = {device.id for device in devices}
        if len(devices) != 20 or len(unique_ids) != 20 or actual != expected:
            raise TenantBindingError("PILOT_DEVICE_SET_INVALID")
        return tuple(sorted(devices, key=lambda device: str(device.id)))

    async def validate_readiness(self, target_device_ids: Sequence[UUID]) -> ValidatedTenantBinding:
        if len(target_device_ids) != 20 or len(set(target_device_ids)) != 20:
            raise TenantBindingError("PILOT_TARGET_COUNT_INVALID")

        discovered_tb_tenant_id = await self._tb.authenticated_tenant_id()
        if discovered_tb_tenant_id != self.config.tb_tenant_id:
            raise TenantBindingError("THINGSBOARD_TENANT_IDENTITY_MISMATCH")
        discovered_cmms_company_id = await self._cmms.authenticated_company_id()
        if discovered_cmms_company_id != self.config.cmms_company_id:
            raise TenantBindingError("CMMS_COMPANY_IDENTITY_MISMATCH")

        alarm_counts = [
            await self._tb.active_pdm_alarm_count(device_id) for device_id in target_device_ids
        ]
        work_order_count = await self._cmms.total_work_orders()
        active_alarm_count = sum(alarm_counts)
        if active_alarm_count != 0 or work_order_count != 0:
            raise TenantBindingError(
                "ISOLATION_BASELINE_NOT_CLEAN",
                {
                    "tb_tenant_id": str(discovered_tb_tenant_id),
                    "active_pdm_alarm_count": active_alarm_count,
                    "cmms_company_id": discovered_cmms_company_id,
                    "work_order_count": work_order_count,
                    "operator_action": "use a clean isolated environment",
                },
            )

        return ValidatedTenantBinding(
            tenant_id=self.config.tenant_id,
            alias=self.config.alias,
            tb_tenant_id=discovered_tb_tenant_id,
            cmms_company_id=discovered_cmms_company_id,
            pdm_credential_ref=self.config.pdm_credential_ref,
            tb_credential_ref=self.config.tb_credential_ref,
            cmms_credential_ref=self.config.cmms_credential_ref,
            cmms_webhook_secret_ref=self.config.cmms_webhook_secret_ref,
        )
