from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.exc import IntegrityError
from testcontainers.community.postgres import PostgresContainer


COMPONENT_ROOT = Path(__file__).parents[2]
TENANT_ID = UUID("00000000-0000-4000-8000-000000000001")
OTHER_TENANT_ID = UUID("00000000-0000-4000-8000-000000000002")
TB_TENANT_ID = UUID("10000000-0000-4000-8000-000000000001")
EQUIPMENT_ID = UUID("20000000-0000-4000-8000-000000000001")
TB_DEVICE_ID = UUID("30000000-0000-4000-8000-000000000001")


def _alembic_config(database_url: str) -> Config:
    config = Config(str(COMPONENT_ROOT / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    return config


@pytest.fixture(scope="module")
def database_url():
    with PostgresContainer("postgres:16-alpine", driver="psycopg") as postgres:
        url = postgres.get_connection_url()
        command.upgrade(_alembic_config(url), "head")
        yield url


def _tenant(**overrides):
    from platform_integration.models.bindings import TenantBinding

    values = {
        "tenant_id": TENANT_ID,
        "alias": "ifactory-pilot",
        "tb_tenant_id": TB_TENANT_ID,
        "cmms_company_id": 101,
        "pdm_credential_ref": "PDM_PILOT_CREDENTIAL",
        "tb_credential_ref": "TB_PILOT_CREDENTIAL",
        "cmms_credential_ref": "CMMS_PILOT_CREDENTIAL",
        "cmms_webhook_secret_ref": None,
        "enabled": True,
    }
    values.update(overrides)
    return TenantBinding(**values)


def _equipment(**overrides):
    from platform_integration.models.bindings import EquipmentMapping

    values = {
        "tenant_id": TENANT_ID,
        "equipment_id": EQUIPMENT_ID,
        "tb_device_id": TB_DEVICE_ID,
        "cmms_asset_id": 501,
        "cmms_request_digest": "a" * 64,
        "cmms_request_summary": {"name_hash": "b" * 64},
        "status": "ACTIVE",
        "enabled": True,
    }
    values.update(overrides)
    return EquipmentMapping(**values)


def _measurement(**overrides):
    from platform_integration.models.bindings import MeasurementBinding

    values = {
        "tenant_id": TENANT_ID,
        "equipment_id": EQUIPMENT_ID,
        "meas_code": "vibration_rms",
        "telemetry_key": "vibration",
        "unit": "mm/s",
        "sampling_frequency": "1min",
        "request_window_points": 66,
        "context_points": 60,
        "horizon_points": 15,
        "value_scale": 2,
        "model_profile_id": "pilot-cnc-vibration",
        "model_info_id": "pilot-fixture-v1-cnc-vibration",
        "preprocessing_version": "pdm-v2-pilot-1",
        "policy_version": "pilot-policy-v1",
        "risk_direction": "ABOVE",
        "risk_threshold": Decimal("7.50"),
        "enabled": True,
    }
    values.update(overrides)
    return MeasurementBinding(**values)


async def _add_tenant(session_factory, binding) -> None:
    from platform_integration.repositories.bindings import BindingRepository

    async with session_factory() as session, session.begin():
        await BindingRepository(session).add_tenant(binding)


async def _add_equipment(session_factory, mapping) -> None:
    from platform_integration.repositories.bindings import BindingRepository

    async with session_factory() as session, session.begin():
        await BindingRepository(session).add_equipment(mapping)


async def _add_measurement(session_factory, binding) -> None:
    from platform_integration.repositories.bindings import BindingRepository

    async with session_factory() as session, session.begin():
        await BindingRepository(session).add_measurement(binding)


async def test_tenant_identities_are_independently_unique_and_enabled_is_required(database_url):
    """Weakening any tenant identity uniqueness or enabled nullability permits ambiguous routing."""
    from platform_integration.db import create_async_sessionmaker

    session_factory = create_async_sessionmaker(database_url)
    try:
        await _add_tenant(session_factory, _tenant())

        duplicates = (
            _tenant(
                alias="second-alias",
                tb_tenant_id=UUID("10000000-0000-4000-8000-000000000002"),
                cmms_company_id=102,
            ),
            _tenant(
                tenant_id=OTHER_TENANT_ID,
                alias="third-alias",
                cmms_company_id=103,
            ),
            _tenant(
                tenant_id=UUID("00000000-0000-4000-8000-000000000005"),
                tb_tenant_id=UUID("10000000-0000-4000-8000-000000000005"),
                cmms_company_id=105,
            ),
            _tenant(
                tenant_id=UUID("00000000-0000-4000-8000-000000000003"),
                alias="fourth-alias",
                tb_tenant_id=UUID("10000000-0000-4000-8000-000000000003"),
            ),
            _tenant(
                tenant_id=UUID("00000000-0000-4000-8000-000000000004"),
                alias="fifth-alias",
                tb_tenant_id=UUID("10000000-0000-4000-8000-000000000004"),
                cmms_company_id=104,
                enabled=None,
            ),
        )
        for duplicate in duplicates:
            with pytest.raises(IntegrityError):
                await _add_tenant(session_factory, duplicate)
    finally:
        await session_factory.kw["bind"].dispose()


async def test_equipment_identity_is_unique_within_each_tenant(database_url):
    """Dropping any scoped equipment identity constraint permits split-brain asset mappings."""
    from platform_integration.db import create_async_sessionmaker

    session_factory = create_async_sessionmaker(database_url)
    try:
        await _add_equipment(session_factory, _equipment())

        for duplicate in (
            _equipment(
                tb_device_id=UUID("30000000-0000-4000-8000-000000000002"),
                cmms_asset_id=502,
            ),
            _equipment(
                equipment_id=UUID("20000000-0000-4000-8000-000000000002"),
                cmms_asset_id=503,
            ),
            _equipment(
                equipment_id=UUID("20000000-0000-4000-8000-000000000003"),
                tb_device_id=UUID("30000000-0000-4000-8000-000000000003"),
            ),
        ):
            with pytest.raises(IntegrityError):
                await _add_equipment(session_factory, duplicate)
    finally:
        await session_factory.kw["bind"].dispose()


async def test_measurement_identity_is_unique_by_code_and_telemetry_key(database_url):
    """Dropping either scoped measurement key permits conflicting model input bindings."""
    from platform_integration.db import create_async_sessionmaker

    session_factory = create_async_sessionmaker(database_url)
    try:
        await _add_measurement(session_factory, _measurement())

        with pytest.raises(IntegrityError):
            await _add_measurement(
                session_factory,
                _measurement(telemetry_key="vibration_secondary"),
            )
        with pytest.raises(IntegrityError):
            await _add_measurement(
                session_factory,
                _measurement(meas_code="vibration_peak"),
            )
    finally:
        await session_factory.kw["bind"].dispose()
