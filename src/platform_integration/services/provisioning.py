from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID, uuid5

import rfc8785

from platform_integration.clients.cmms import CmmsClientError
from platform_integration.clients.thingsboard import ThingsBoardClientError
from platform_integration.contracts.cmms import CmmsAssetCreate
from platform_integration.services.tenant_bindings import (
    TenantBindingError,
    TenantBindingsService,
    ValidatedTenantBinding,
)


PLAN_LIFETIME = timedelta(minutes=30)


class ProvisioningError(RuntimeError):
    def __init__(self, code: str, details: dict[str, object] | None = None) -> None:
        self.code = code
        self.details = details
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class PilotProfile:
    model_profile_id: str
    model_info_id: str
    meas_code: str
    unit: str
    value_scale: int
    risk_direction: str
    risk_threshold: str

    def measurement_binding(self) -> dict[str, object]:
        return {
            "meas_code": self.meas_code,
            "telemetry_key": self.meas_code,
            "unit": self.unit,
            "sampling_frequency": "1min",
            "request_window_points": 66,
            "context_points": 60,
            "horizon_points": 15,
            "value_scale": self.value_scale,
            "model_profile_id": self.model_profile_id,
            "model_info_id": self.model_info_id,
            "preprocessing_version": "pdm-v2-pilot-1",
            "policy_version": "pilot-threshold-v1",
            "predictor_kind": "repeat-last",
            "risk_direction": self.risk_direction,
            "risk_threshold": self.risk_threshold,
            "enabled": True,
        }


PROFILES: Mapping[str, PilotProfile] = {
    "CNC": PilotProfile(
        "pilot-cnc-vibration",
        "pilot-fixture-v1-cnc-vibration",
        "vibration_rms",
        "mm/s",
        2,
        "ABOVE",
        "5.00",
    ),
    "INJECTION_MOLDING": PilotProfile(
        "pilot-injection-pressure",
        "pilot-fixture-v1-injection-pressure",
        "injection_pressure",
        "bar",
        1,
        "ABOVE",
        "175.0",
    ),
    "ASSEMBLY_ROBOT": PilotProfile(
        "pilot-robot-position",
        "pilot-fixture-v1-robot-position",
        "position_deviation",
        "mm",
        3,
        "ABOVE",
        "1.000",
    ),
    "TIGHTENING": PilotProfile(
        "pilot-tightening-torque",
        "pilot-fixture-v1-tightening-torque",
        "torque",
        "N·m",
        2,
        "BELOW",
        "15.00",
    ),
    "AIR_COMPRESSOR": PilotProfile(
        "pilot-compressor-pressure",
        "pilot-fixture-v1-compressor-pressure",
        "discharge_pressure",
        "MPa",
        3,
        "BELOW",
        "0.500",
    ),
    "EOL_TESTER": PilotProfile(
        "pilot-eol-pass-rate",
        "pilot-fixture-v1-eol-pass-rate",
        "pass_rate",
        "%",
        2,
        "BELOW",
        "90.00",
    ),
}


@dataclass(frozen=True, slots=True)
class ProvisioningTarget:
    tb_device_id: UUID
    device_name: str
    device_type: str
    asset_name: str
    equipment_id: UUID
    measurement_binding: dict[str, object]

    def canonical(self) -> dict[str, object]:
        return {
            "tb_device_id": str(self.tb_device_id),
            "device_name": self.device_name,
            "device_type": self.device_type,
            "asset_name": self.asset_name,
            "equipment_id": str(self.equipment_id),
            "measurement_binding": self.measurement_binding,
        }


@dataclass(slots=True)
class ProvisioningPlan:
    tenant_id: UUID
    plan_hash: str
    canonical: dict[str, Any]
    tenant_snapshot: dict[str, object]
    targets: tuple[ProvisioningTarget, ...]
    actor: UUID
    created_at: datetime
    expires_at: datetime
    apply_actor: UUID | None = None
    applied_at: datetime | None = None
    target_results: dict[str, dict[str, object]] | None = None
    terminal_result: str | None = None


ProvisioningResult = ProvisioningPlan


class ProvisioningStore(Protocol):
    async def save_plan(self, snapshot: ValidatedTenantBinding, plan: ProvisioningPlan) -> None: ...

    async def load_plan(self, plan_hash: str) -> ProvisioningPlan | None: ...

    async def preflight_plan(
        self,
        plan: ProvisioningPlan,
        reservations: Sequence[tuple[ProvisioningTarget, str, dict[str, object]]],
    ) -> dict[UUID, int | None]: ...

    async def start_apply(self, plan: ProvisioningPlan, actor: UUID, now: datetime) -> None: ...

    async def abort_apply(self, plan: ProvisioningPlan) -> None: ...

    async def reserve_target(
        self,
        plan: ProvisioningPlan,
        target: ProvisioningTarget,
        request_digest: str,
        request_summary: dict[str, object],
    ) -> None: ...

    async def activate_target(
        self,
        plan: ProvisioningPlan,
        target: ProvisioningTarget,
        cmms_asset_id: int,
        measurement: dict[str, object],
        actor: UUID,
    ) -> None: ...

    async def finish_apply(
        self,
        plan: ProvisioningPlan,
        actor: UUID,
        results: dict[str, dict[str, object]],
        now: datetime,
    ) -> None: ...

    async def read_receipt(self, tenant_id: UUID, plan_hash: str) -> ProvisioningPlan | None: ...


class ThingsBoardProvisioning(Protocol):
    async def list_devices(self) -> Sequence[Any]: ...

    async def write_asset_attributes(
        self,
        device_id: UUID,
        *,
        equipment_id: UUID,
        cmms_asset_id: int,
    ) -> None: ...

    async def read_asset_attributes(self, device_id: UUID) -> Any: ...


class CmmsProvisioning(Protocol):
    async def find_asset_by_equipment_id(self, equipment_id: UUID) -> Any | None: ...

    async def create_asset(self, request: CmmsAssetCreate, *, idempotency_key: str) -> Any: ...


def canonical_plan_hash(canonical: Mapping[str, object]) -> str:
    return hashlib.sha256(rfc8785.dumps(dict(canonical))).hexdigest()


def _request_projection(target: ProvisioningTarget) -> dict[str, object]:
    return {"name": target.asset_name, "equipment_id": str(target.equipment_id)}


def _request_digest(target: ProvisioningTarget) -> str:
    return hashlib.sha256(rfc8785.dumps(_request_projection(target))).hexdigest()


def _binding_digest(binding: Mapping[str, object]) -> str:
    return hashlib.sha256(rfc8785.dumps(dict(binding))).hexdigest()


class ProvisioningService:
    def __init__(
        self,
        *,
        tenant_bindings: TenantBindingsService,
        store: ProvisioningStore,
        thingsboard: ThingsBoardProvisioning,
        cmms: CmmsProvisioning,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._tenant_bindings = tenant_bindings
        self._store = store
        self._tb = thingsboard
        self._cmms = cmms
        self._now = now or (lambda: datetime.now(UTC))

    async def build_plan(self, tenant_id: UUID, actor: UUID) -> ProvisioningPlan:
        if tenant_id != self._tenant_bindings.config.tenant_id:
            raise ProvisioningError("PILOT_ALIAS_TENANT_MISMATCH")
        devices = await self._tb.list_devices()
        try:
            targets_devices = self._tenant_bindings.validate_target_devices(devices)
            snapshot = await self._tenant_bindings.validate_readiness(
                tuple(device.id for device in targets_devices)
            )
        except TenantBindingError as exc:
            raise ProvisioningError(exc.code, exc.details) from None

        targets = tuple(
            ProvisioningTarget(
                tb_device_id=device.id,
                device_name=device.name,
                device_type=device.device_type,
                asset_name=device.name,
                equipment_id=uuid5(tenant_id, f"pilot-equipment:{device.id}"),
                measurement_binding=PROFILES[device.device_type].measurement_binding(),
            )
            for device in targets_devices
        )
        canonical = {
            "schema_version": 1,
            "tenant_id": str(tenant_id),
            "tenant_alias": snapshot.alias,
            "tb_tenant_id": str(snapshot.tb_tenant_id),
            "cmms_company_id": snapshot.cmms_company_id,
            "targets": [target.canonical() for target in targets],
        }
        created_at = self._now()
        plan = ProvisioningPlan(
            tenant_id=tenant_id,
            plan_hash=canonical_plan_hash(canonical),
            canonical=canonical,
            tenant_snapshot={
                "tenant_id": str(snapshot.tenant_id),
                "alias": snapshot.alias,
                "tb_tenant_id": str(snapshot.tb_tenant_id),
                "cmms_company_id": snapshot.cmms_company_id,
            },
            targets=targets,
            actor=actor,
            created_at=created_at,
            expires_at=created_at + PLAN_LIFETIME,
        )
        await self._store.save_plan(snapshot, plan)
        return plan

    async def apply(self, plan_hash: str, confirmed_hash: str, actor: UUID) -> ProvisioningResult:
        plan = await self._store.load_plan(plan_hash)
        if plan is None:
            raise ProvisioningError("PROVISIONING_PLAN_NOT_FOUND")
        if confirmed_hash != plan_hash or confirmed_hash != plan.plan_hash:
            raise ProvisioningError("PROVISIONING_CONFIRMATION_MISMATCH")
        now = self._now()
        if now >= plan.expires_at:
            raise ProvisioningError("PROVISIONING_PLAN_EXPIRED")
        if plan.applied_at is not None or plan.terminal_result is not None:
            raise ProvisioningError("PROVISIONING_PLAN_ALREADY_APPLIED")
        if canonical_plan_hash(plan.canonical) != plan.plan_hash:
            raise ProvisioningError("PROVISIONING_PLAN_DRIFTED")

        reservations = tuple(
            (target, _request_digest(target), _request_projection(target))
            for target in plan.targets
        )
        persisted_asset_ids = await self._store.preflight_plan(plan, reservations)

        devices = await self._tb.list_devices()
        try:
            validated = self._tenant_bindings.validate_target_devices(devices)
        except TenantBindingError as exc:
            raise ProvisioningError(exc.code, exc.details) from None
        expected = {
            (target.tb_device_id, target.device_name, target.device_type) for target in plan.targets
        }
        actual = {(device.id, device.name, device.device_type) for device in validated}
        if actual != expected:
            raise ProvisioningError("PROVISIONING_TARGET_DRIFTED")
        try:
            snapshot = await self._tenant_bindings.validate_readiness(
                tuple(target.tb_device_id for target in plan.targets)
            )
        except TenantBindingError as exc:
            raise ProvisioningError(exc.code, exc.details) from None
        if (
            str(snapshot.tb_tenant_id) != plan.canonical["tb_tenant_id"]
            or snapshot.cmms_company_id != plan.canonical["cmms_company_id"]
        ):
            raise ProvisioningError("PROVISIONING_TENANT_DRIFTED")

        await self._store.start_apply(plan, actor, now)
        results: dict[str, dict[str, object]] = {}
        completed = False
        try:
            for target, request_digest, request_summary in reservations:
                await self._apply_target(
                    plan,
                    target,
                    request_digest,
                    request_summary,
                    persisted_asset_ids.get(target.tb_device_id),
                    actor,
                    results,
                )
            completed_at = self._now()
            await self._store.finish_apply(plan, actor, results, completed_at)
            completed = True
            return plan
        finally:
            if not completed:
                await self._store.abort_apply(plan)

    async def _apply_target(
        self,
        plan: ProvisioningPlan,
        target: ProvisioningTarget,
        request_digest: str,
        request_summary: dict[str, object],
        persisted_cmms_asset_id: int | None,
        actor: UUID,
        results: dict[str, dict[str, object]],
    ) -> None:
        await self._store.reserve_target(plan, target, request_digest, request_summary)
        asset = await self._cmms.find_asset_by_equipment_id(target.equipment_id)
        if persisted_cmms_asset_id is not None and (
            asset is None or asset.id != persisted_cmms_asset_id
        ):
            raise ProvisioningError("CMMS_ASSET_CONFLICT")
        if asset is None:
            request = CmmsAssetCreate(
                name=target.asset_name,
                equipment_id=target.equipment_id,
            )
            try:
                asset = await self._cmms.create_asset(
                    request,
                    idempotency_key=f"pilot-asset:{target.tb_device_id}",
                )
            except CmmsClientError as exc:
                if exc.code != "CMMS_WRITE_RESULT_UNKNOWN":
                    raise ProvisioningError(exc.code) from None
                asset = await self._cmms.find_asset_by_equipment_id(target.equipment_id)
                if asset is None:
                    raise ProvisioningError("CMMS_WRITE_RESULT_UNKNOWN") from None
        if asset.equipment_id != target.equipment_id or asset.name != target.asset_name:
            raise ProvisioningError("CMMS_ASSET_CONFLICT")

        readback = None
        try:
            await self._tb.write_asset_attributes(
                target.tb_device_id,
                equipment_id=target.equipment_id,
                cmms_asset_id=asset.id,
            )
        except ThingsBoardClientError as exc:
            if exc.code != "THINGSBOARD_WRITE_RESULT_UNKNOWN":
                raise ProvisioningError(exc.code) from None
            readback = await self._tb.read_asset_attributes(target.tb_device_id)
        if readback is None:
            readback = await self._tb.read_asset_attributes(target.tb_device_id)
        if readback.equipment_id != target.equipment_id or readback.cmms_asset_id != asset.id:
            raise ProvisioningError("THINGSBOARD_ATTRIBUTE_CONFLICT")

        confirmed_asset = await self._cmms.find_asset_by_equipment_id(target.equipment_id)
        if (
            confirmed_asset is None
            or confirmed_asset.id != asset.id
            or confirmed_asset.name != target.asset_name
            or confirmed_asset.equipment_id != target.equipment_id
        ):
            raise ProvisioningError("CMMS_ASSET_READBACK_CONFLICT")
        await self._store.activate_target(
            plan,
            target,
            asset.id,
            target.measurement_binding,
            actor,
        )
        results[str(target.tb_device_id)] = {
            "tb_device_id": str(target.tb_device_id),
            "equipment_id": str(target.equipment_id),
            "cmms_asset_id": asset.id,
            "cmms_request_digest": request_digest,
            "measurement_binding_digest": _binding_digest(target.measurement_binding),
            "cmms_result_code": "VERIFIED",
            "thingsboard_result_code": "VERIFIED",
            "result_code": "ACTIVE",
        }

    async def verify(self, tenant_id: UUID, plan_hash: str) -> ProvisioningResult:
        receipt = await self._store.read_receipt(tenant_id, plan_hash)
        if receipt is None:
            raise ProvisioningError("PROVISIONING_RECEIPT_NOT_FOUND")
        if (
            receipt.target_results is None
            or len(receipt.target_results) != 20
            or set(receipt.target_results)
            != {str(target.tb_device_id) for target in receipt.targets}
            or any(
                result.get("result_code") != "ACTIVE" for result in receipt.target_results.values()
            )
        ):
            raise ProvisioningError("PROVISIONING_RECEIPT_INCOMPLETE")
        return receipt
