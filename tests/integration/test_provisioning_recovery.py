from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from testcontainers.community.postgres import PostgresContainer

from platform_integration.clients.cmms import CmmsClientError
from platform_integration.clients.thingsboard import ThingsBoardClientError
from platform_integration.db import create_async_sessionmaker
from platform_integration.services.provisioning import ProvisioningError

from tests.services.test_provisioning import ACTOR, FakeCmms, FakeStore, FakeThingsBoard, service
from platform_integration.services.tenant_bindings import ISOLATED_TENANT_ID


COMPONENT_ROOT = Path(__file__).parents[2]


class ResponseLossCmms(FakeCmms):
    def __init__(self) -> None:
        super().__init__()
        self.response_lost = True
        self.events = []

    async def find_asset_by_equipment_id(self, equipment_id):
        self.events.append(("get", equipment_id))
        return await super().find_asset_by_equipment_id(equipment_id)

    async def create_asset(self, request, *, idempotency_key):
        self.events.append(("post", request.equipment_id, idempotency_key))
        asset = await super().create_asset(request, idempotency_key=idempotency_key)
        if self.response_lost:
            self.response_lost = False
            raise CmmsClientError("CMMS_WRITE_RESULT_UNKNOWN")
        return asset


class ResponseLossThingsBoard(FakeThingsBoard):
    def __init__(self) -> None:
        super().__init__()
        self.response_lost = True

    async def write_asset_attributes(self, device_id, *, equipment_id, cmms_asset_id):
        await super().write_asset_attributes(
            device_id,
            equipment_id=equipment_id,
            cmms_asset_id=cmms_asset_id,
        )
        if self.response_lost:
            self.response_lost = False
            raise ThingsBoardClientError("THINGSBOARD_WRITE_RESULT_UNKNOWN")


@pytest.mark.asyncio
async def test_response_loss_recovers_by_exact_readback_without_blind_post() -> None:
    """Retrying the CMMS POST after response loss could create a duplicate asset."""
    tb = ResponseLossThingsBoard()
    cmms = ResponseLossCmms()
    store = FakeStore()
    provisioning, _, _, _ = service(tb=tb, cmms=cmms, store=store)
    plan = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)

    result = await provisioning.apply(plan.plan_hash, plan.plan_hash, ACTOR)

    assert result.terminal_result == "ACTIVE"
    assert len(result.target_results) == 20
    assert len(cmms.create_calls) == 20
    first_target = plan.targets[0]
    first_reserve_index = next(
        index
        for index, event in enumerate(store.events)
        if event[0] == "reserve" and event[1] == first_target.tb_device_id
    )
    first_post_index = cmms.events.index(
        (
            "post",
            first_target.equipment_id,
            f"pilot-asset:{first_target.tb_device_id}",
        )
    )
    assert first_reserve_index >= 0
    assert first_post_index >= 0
    assert [event[0] for event in cmms.events[:3]] == ["get", "post", "get"]
    assert sum(event[0] == "post" for event in cmms.events) == 20
    assert len(tb.write_calls) == 20

    first_binding = store.bindings[first_target.tb_device_id]
    assert first_binding["request_window_points"] == 66
    assert first_binding == first_target.measurement_binding
    assert all(
        reservation[0] == target.equipment_id
        for target, reservation in zip(
            plan.targets,
            (store.reservations[target.tb_device_id] for target in plan.targets),
            strict=True,
        )
    )


@pytest.mark.asyncio
async def test_rerun_reuses_reserved_uuid_and_rejects_conflicting_binding() -> None:
    """Replacing a persisted reservation/binding would hide drift and break replay identity."""
    tb = FakeThingsBoard()
    cmms = FakeCmms()
    store = FakeStore()
    provisioning, _, _, _ = service(tb=tb, cmms=cmms, store=store)
    plan = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)
    target = plan.targets[0]
    digest = "0" * 64
    store.reservations[target.tb_device_id] = (
        UUID("00000000-0000-4000-8000-000000009999"),
        digest,
        {"name": target.asset_name, "equipment_id": str(target.equipment_id)},
    )

    with pytest.raises(ProvisioningError, match="EQUIPMENT_RESERVATION_CONFLICT"):
        await provisioning.apply(plan.plan_hash, plan.plan_hash, ACTOR)
    assert cmms.create_calls == []

    store.reservations.clear()
    store.bindings[target.tb_device_id] = {
        **target.measurement_binding,
        "request_window_points": 65,
    }
    previous_create_count = len(cmms.create_calls)
    with pytest.raises(ProvisioningError, match="MEASUREMENT_BINDING_CONFLICT"):
        await provisioning.apply(plan.plan_hash, plan.plan_hash, ACTOR)
    assert store.bindings[target.tb_device_id]["request_window_points"] == 65
    assert len(cmms.create_calls) == previous_create_count


@pytest.mark.asyncio
async def test_sql_store_persists_secret_free_complete_receipt_across_instances() -> None:
    """An in-memory receipt or split terminal write would not survive process replacement."""
    from platform_integration.repositories.provisioning import SqlProvisioningStore

    with PostgresContainer("postgres:16-alpine", driver="psycopg") as postgres:
        database_url = postgres.get_connection_url()
        alembic = Config(str(COMPONENT_ROOT / "alembic.ini"))
        alembic.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
        command.upgrade(alembic, "head")
        sessions = create_async_sessionmaker(database_url)
        tb = FakeThingsBoard()
        cmms = FakeCmms()
        first_store = SqlProvisioningStore(sessions)
        provisioning, _, _, _ = service(tb=tb, cmms=cmms, store=first_store)
        plan = await provisioning.build_plan(ISOLATED_TENANT_ID, ACTOR)

        replacement_store = SqlProvisioningStore(sessions)
        replacement, _, _, _ = service(tb=tb, cmms=cmms, store=replacement_store)
        await replacement.apply(plan.plan_hash, plan.plan_hash, ACTOR)
        receipt = await replacement.verify(ISOLATED_TENANT_ID, plan.plan_hash)

        assert receipt.terminal_result == "ACTIVE"
        assert receipt.target_results is not None
        assert len(receipt.target_results) == 20
        engine = create_engine(database_url)
        try:
            with engine.connect() as connection:
                counts = (
                    connection.execute(
                        text(
                            """
                        SELECT
                          (SELECT count(*) FROM equipment_mapping) AS equipment,
                          (SELECT count(*) FROM measurement_binding) AS measurement,
                          (SELECT count(*) FROM provisioning_plan
                             WHERE terminal_result = 'ACTIVE') AS receipts
                        """
                        )
                    )
                    .mappings()
                    .one()
                )
                serialized = connection.execute(
                    text(
                        "SELECT canonical_plan::text || tenant_snapshot::text || "
                        "target_results::text FROM provisioning_plan"
                    )
                ).scalar_one()
        finally:
            engine.dispose()
            await sessions.kw["bind"].dispose()

    assert dict(counts) == {"equipment": 20, "measurement": 20, "receipts": 1}
    assert "secret" not in serialized.lower()
    assert "token" not in serialized.lower()
