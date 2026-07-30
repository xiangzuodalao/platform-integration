from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import IntegrityError
from testcontainers.community.postgres import PostgresContainer


COMPONENT_ROOT = Path(__file__).parents[2]
EXPECTED_TABLES = {
    "tenant_binding",
    "equipment_mapping",
    "measurement_binding",
    "provisioning_plan",
    "prediction_run",
    "risk_evaluation_state",
    "audit_event",
}
MEASUREMENT_COLUMNS = {
    "tenant_id",
    "equipment_id",
    "meas_code",
    "telemetry_key",
    "unit",
    "sampling_frequency",
    "request_window_points",
    "context_points",
    "horizon_points",
    "value_scale",
    "model_profile_id",
    "model_info_id",
    "preprocessing_version",
    "policy_version",
    "risk_direction",
    "risk_threshold",
    "enabled",
}


def _alembic_config(database_url: str) -> Config:
    config = Config(str(COMPONENT_ROOT / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    return config


def test_database_url_is_an_external_secret_setting():
    """Rejecting the setting or exposing its password breaks safe database configuration."""
    from platform_integration.config import Settings

    password = "redaction-sentinel"
    database_url = URL.create(
        "postgresql+psycopg",
        username="integration",
        password=password,
        host="database",
        database="integration",
    ).render_as_string(hide_password=False)
    settings = Settings(database_url=database_url)

    assert settings.database_url is not None
    assert settings.database_url.get_secret_value() == database_url
    assert password not in repr(settings)


@pytest.fixture(scope="module")
def postgres_url():
    with PostgresContainer("postgres:16-alpine", driver="psycopg") as postgres:
        database_url = postgres.get_connection_url()
        command.upgrade(_alembic_config(database_url), "head")
        yield database_url


def test_shadow_schema_uses_contract_types_and_bounded_summaries(postgres_url):
    """Changing domain IDs, CMMS IDs, timestamps, numerics, or summary storage breaks persistence."""
    engine = create_engine(postgres_url)
    try:
        inspector = inspect(engine)
        assert EXPECTED_TABLES <= set(inspector.get_table_names())
        assert {column["name"] for column in inspector.get_columns("measurement_binding")} == (
            MEASUREMENT_COLUMNS
        )

        with engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT table_name, column_name, data_type, udt_name,
                           numeric_precision, numeric_scale, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = ANY(:tables)
                    """
                ),
                {"tables": sorted(EXPECTED_TABLES)},
            ).mappings()
            columns = {(row["table_name"], row["column_name"]): row for row in rows}

        for table_name, column_name in (
            ("tenant_binding", "tenant_id"),
            ("tenant_binding", "tb_tenant_id"),
            ("equipment_mapping", "equipment_id"),
            ("equipment_mapping", "tb_device_id"),
            ("prediction_run", "run_id"),
            ("prediction_run", "correlation_id"),
            ("audit_event", "actor_id"),
            ("provisioning_plan", "apply_actor_id"),
        ):
            assert columns[(table_name, column_name)]["udt_name"] == "uuid"

        for table_name, column_name in (
            ("tenant_binding", "cmms_company_id"),
            ("equipment_mapping", "cmms_asset_id"),
            ("provisioning_plan", "cmms_company_id"),
        ):
            assert columns[(table_name, column_name)]["data_type"] == "bigint"

        for table_name, column_name in (
            ("provisioning_plan", "expires_at"),
            ("provisioning_plan", "applied_at"),
            ("prediction_run", "scheduled_at"),
            ("prediction_run", "lease_expires_at"),
            ("prediction_run", "heartbeat_at"),
            ("audit_event", "occurred_at"),
        ):
            assert columns[(table_name, column_name)]["data_type"] == ("timestamp with time zone")

        threshold = columns[("measurement_binding", "risk_threshold")]
        assert threshold["data_type"] == "numeric"
        assert (threshold["numeric_precision"], threshold["numeric_scale"]) == (20, 6)
        internal_active = columns[("risk_evaluation_state", "internal_active")]
        assert internal_active["data_type"] == "boolean"
        assert internal_active["is_nullable"] == "NO"

        jsonb_columns = {key for key, column in columns.items() if column["udt_name"] == "jsonb"}
        assert jsonb_columns == {
            ("equipment_mapping", "cmms_request_summary"),
            ("provisioning_plan", "canonical_plan"),
            ("provisioning_plan", "tenant_snapshot"),
            ("provisioning_plan", "target_results"),
            ("prediction_run", "quality_summary"),
            ("audit_event", "result_summary"),
        }
        forbidden_persistence = {
            "raw_telemetry",
            "history",
            "forecast",
            "connection_url",
            "credential",
            "secret",
        }
        assert not forbidden_persistence.intersection(column_name for _, column_name in columns)

        required_checks = {
            "tenant_binding": {
                "ck_tenant_binding_pdm_credential_ref",
                "ck_tenant_binding_tb_credential_ref",
                "ck_tenant_binding_cmms_credential_ref",
                "ck_tenant_binding_cmms_webhook_secret_ref",
            },
            "equipment_mapping": {
                "ck_equipment_mapping_status",
                "ck_equipment_mapping_summary_size",
            },
            "measurement_binding": {
                "ck_measurement_binding_risk_direction",
                "ck_measurement_binding_threshold_scale",
            },
            "provisioning_plan": {
                "ck_provisioning_plan_canonical_size",
                "ck_provisioning_plan_snapshot_size",
                "ck_provisioning_plan_results_size",
                "ck_provisioning_plan_expiry",
                "ck_provisioning_plan_apply_actor",
            },
            "prediction_run": {
                "ck_prediction_run_status",
                "ck_prediction_run_attempt",
                "ck_prediction_run_quality_summary_size",
            },
            "audit_event": {"ck_audit_event_result_summary_size"},
        }
        for table_name, names in required_checks.items():
            assert names <= {
                constraint["name"] for constraint in inspector.get_check_constraints(table_name)
            }
    finally:
        engine.dispose()


def test_provisioning_receipt_expires_exactly_thirty_minutes_after_creation(postgres_url):
    """Allowing a longer-lived confirmation plan weakens the required apply window."""
    engine = create_engine(postgres_url)
    statement = text(
        """
        INSERT INTO provisioning_plan (
            tenant_id, plan_hash, canonical_plan, tenant_snapshot,
            tb_tenant_id, cmms_company_id, actor_id, created_at, expires_at
        ) VALUES (
            '60000000-0000-4000-8000-000000000001',
            :plan_hash,
            CAST(:canonical_plan AS jsonb),
            CAST(:tenant_snapshot AS jsonb),
            '70000000-0000-4000-8000-000000000001',
            201,
            '80000000-0000-4000-8000-000000000001',
            TIMESTAMPTZ '2026-07-30 06:00:00+00',
            :expires_at
        )
        """
    )
    try:
        with pytest.raises(IntegrityError), engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "plan_hash": "a" * 64,
                    "canonical_plan": '{"targets":[]}',
                    "tenant_snapshot": '{"alias":"ifactory-pilot"}',
                    "expires_at": datetime(2026, 7, 30, 6, 31, tzinfo=UTC),
                },
            )
        with engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "plan_hash": "b" * 64,
                    "canonical_plan": '{"targets":[]}',
                    "tenant_snapshot": '{"alias":"ifactory-pilot"}',
                    "expires_at": datetime(2026, 7, 30, 6, 0, tzinfo=UTC) + timedelta(minutes=30),
                },
            )
    finally:
        engine.dispose()


def test_shadow_migration_is_reversible_using_only_the_testcontainer_url(postgres_url, monkeypatch):
    """A downgrade, re-upgrade, or loss of an existing apply actor breaks safe rollout."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("PLATFORM_INTEGRATION_DATABASE_URL", raising=False)
    config = _alembic_config(postgres_url)

    command.downgrade(config, "base")
    engine = create_engine(postgres_url)
    try:
        assert EXPECTED_TABLES.isdisjoint(inspect(engine).get_table_names())
    finally:
        engine.dispose()

    command.upgrade(config, "0001_shadow_foundation")
    engine = create_engine(postgres_url)
    try:
        tenant_id = "00000000-0000-4000-8000-000000000001"
        build_actor = "00000000-0000-4000-8000-000000000097"
        apply_actor = "00000000-0000-4000-8000-000000000098"
        plan_hash = "c" * 64
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO provisioning_plan (
                        tenant_id, plan_hash, canonical_plan, tenant_snapshot,
                        tb_tenant_id, cmms_company_id, actor_id, created_at,
                        expires_at, applied_at, target_results, terminal_result
                    ) VALUES (
                        :tenant_id, :plan_hash, '{"targets":[]}', '{"alias":"pilot"}',
                        :tenant_id, 201, :build_actor, :created_at,
                        :expires_at, :applied_at, '{}', 'ACTIVE'
                    )
                    """
                ),
                {
                    "tenant_id": tenant_id,
                    "plan_hash": plan_hash,
                    "build_actor": build_actor,
                    "created_at": datetime(2026, 7, 30, 6, 0, tzinfo=UTC),
                    "expires_at": datetime(2026, 7, 30, 6, 30, tzinfo=UTC),
                    "applied_at": datetime(2026, 7, 30, 6, 5, tzinfo=UTC),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO audit_event (
                        tenant_id, actor_id, action, target_type, target_id,
                        correlation_id, result_code, result_summary
                    ) VALUES (
                        :tenant_id, :apply_actor, 'PILOT_PROVISIONING_COMPLETED',
                        'PROVISIONING_PLAN', :plan_hash, :tenant_id, 'ACTIVE', '{}'
                    )
                    """
                ),
                {
                    "tenant_id": tenant_id,
                    "plan_hash": plan_hash,
                    "apply_actor": apply_actor,
                },
            )
        command.upgrade(config, "head")
        assert EXPECTED_TABLES <= set(inspect(engine).get_table_names())
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT apply_actor_id::text FROM provisioning_plan "
                        "WHERE plan_hash = :plan_hash"
                    ),
                    {"plan_hash": plan_hash},
                ).scalar_one()
                == apply_actor
            )
    finally:
        engine.dispose()


def test_internal_risk_state_migration_round_trip(postgres_url):
    """A one-way active-state column would make rollback of the isolated pilot unsafe."""
    config = _alembic_config(postgres_url)
    command.upgrade(config, "head")
    command.downgrade(config, "0002_provisioning_apply_actor")
    engine = create_engine(postgres_url)
    try:
        assert "internal_active" not in {
            column["name"] for column in inspect(engine).get_columns("risk_evaluation_state")
        }
    finally:
        engine.dispose()

    command.upgrade(config, "head")
    engine = create_engine(postgres_url)
    try:
        columns = {
            column["name"]: column
            for column in inspect(engine).get_columns("risk_evaluation_state")
        }
        assert not columns["internal_active"]["nullable"]
    finally:
        engine.dispose()
