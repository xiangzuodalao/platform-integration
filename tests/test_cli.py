import importlib
import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import FastAPI


sys.path.insert(0, str(Path(__file__).parents[1] / "src"))


def require_module(name: str, behaviour: str):
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:
        target_or_parent = {
            ".".join(name.split(".")[:index]) for index in range(1, len(name.split(".")) + 1)
        }
        if exc.name not in target_or_parent:
            raise
        assert False, f"{behaviour} is unavailable: {name} has not been implemented"


def test_serve_parser_defaults_to_8080_and_accepts_a_port_override():
    """A changed serve default or ignored --port argument must fail this contract."""
    cli = require_module("platform_integration.cli", "platform-integration serve CLI")
    parser = cli.build_parser()

    assert parser.parse_args(["serve"]).port == 8080
    assert parser.parse_args(["serve", "--port", "9010"]).port == 9010


def test_provisioning_parsers_require_explicit_confirmation_and_exact_receipt_key():
    """Making confirmation optional or verify broad would weaken the two-turn safety gate."""
    cli = require_module("platform_integration.cli", "provisioning CLI")
    parser = cli.build_parser()

    plan = parser.parse_args(
        ["provision-plan", "--tenant-alias", "ifactory-pilot", "--actor", "operator"]
    )
    assert (plan.command, plan.tenant_alias, plan.actor) == (
        "provision-plan",
        "ifactory-pilot",
        "operator",
    )
    apply = parser.parse_args(
        [
            "provision-apply",
            "--tenant-alias",
            "ifactory-pilot",
            "--plan-hash",
            "a" * 64,
            "--confirmed-hash",
            "a" * 64,
            "--actor",
            "operator",
        ]
    )
    assert apply.plan_hash == apply.confirmed_hash == "a" * 64
    verify = parser.parse_args(
        [
            "provision-verify",
            "--tenant-alias",
            "ifactory-pilot",
            "--plan-hash",
            "a" * 64,
        ]
    )
    assert (verify.command, verify.plan_hash) == ("provision-verify", "a" * 64)


def test_serve_help_does_not_disclose_a_credential_value(capsys):
    """Adding credential material to public CLI help must fail this safety contract."""
    cli = require_module("platform_integration.cli", "platform-integration serve CLI")
    parser = cli.build_parser()

    try:
        parser.parse_args(["serve", "--help"])
    except SystemExit as exc:
        assert exc.code == 0

    assert "credential" not in capsys.readouterr().out.lower()


def test_main_passes_explicit_serve_address_to_uvicorn_without_disclosing_credentials(
    monkeypatch, caplog, capsys
):
    """Dropping a serve override or logging a credential must fail this CLI boundary contract."""
    cli = require_module("platform_integration.cli", "platform-integration serve CLI")
    credential_sentinel = "PDM_CLI_BOUNDARY_SENTINEL"
    calls = []

    def record_run(application, *, host, port):
        calls.append((application, host, port))

    monkeypatch.setenv("PLATFORM_INTEGRATION_PDM_CREDENTIAL_REF", credential_sentinel)
    monkeypatch.setattr(
        sys,
        "argv",
        ["platform-integration", "serve", "--host", "127.0.0.2", "--port", "9031"],
    )
    monkeypatch.setattr(cli.uvicorn, "run", record_run)
    caplog.set_level(logging.DEBUG)

    cli.main()

    assert len(calls) == 1
    application, host, port = calls[0]
    assert isinstance(application, FastAPI)
    assert host == "127.0.0.2"
    assert port == 9031
    assert application.state.settings.pdm_credential_ref == credential_sentinel
    captured = capsys.readouterr()
    assert credential_sentinel not in captured.out
    assert credential_sentinel not in captured.err
    assert credential_sentinel not in caplog.text


def test_provisioning_main_maps_unknown_failures_without_traceback_or_sensitive_details(
    monkeypatch, capsys
):
    """An unhandled provider/database error could print secrets and absolute paths."""
    cli = require_module("platform_integration.cli", "redacted provisioning CLI errors")
    secret = "cli-sensitive-canary"
    absolute_path = "/home/operator/private/runtime.env"

    def fail(*_):
        raise RuntimeError(f"{secret} at {absolute_path}")

    monkeypatch.setattr(cli, "run_provision_plan", fail)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "platform-integration",
            "provision-plan",
            "--tenant-alias",
            "ifactory-pilot",
            "--actor",
            "operator",
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    captured = capsys.readouterr()
    exposed = captured.out + captured.err
    assert exc_info.value.code == 2
    assert captured.err == "error: PROVISIONING_COMMAND_FAILED\n"
    assert secret not in exposed
    assert absolute_path not in exposed
    assert "Traceback" not in exposed


def test_provisioning_receipt_output_includes_exact_apply_actor():
    """Omitting the confirmer from CLI output makes the durable receipt incomplete."""
    provision = require_module(
        "platform_integration.commands.provision", "provisioning receipt output"
    )
    apply_actor = UUID("00000000-0000-4000-8000-000000000098")

    receipt = provision._receipt(
        SimpleNamespace(
            tenant_id=UUID("00000000-0000-4000-8000-000000000001"),
            plan_hash="a" * 64,
            applied_at=None,
            apply_actor=apply_actor,
            terminal_result="ACTIVE",
            target_results={},
        )
    )

    assert receipt["apply_actor_id"] == str(apply_actor)


def test_shadow_role_parsers_expose_only_bounded_explicit_operations():
    """Dropping --once or broadening summary selection would make pilot execution unbounded."""
    cli = require_module("platform_integration.cli", "shadow execution CLI roles")
    parser = cli.build_parser()

    assert parser.parse_args(["migrate"]).command == "migrate"
    discover = parser.parse_args(["discover-identities", "--format", "env"])
    assert (discover.command, discover.format) == ("discover-identities", "env")
    scheduler = parser.parse_args(["scheduler", "--once", "--now", "2026-07-30T06:00:00Z"])
    assert scheduler.once and scheduler.now == "2026-07-30T06:00:00Z"
    assert parser.parse_args(["prediction-worker", "--once"]).once
    summary = parser.parse_args(
        [
            "shadow-summary",
            "--tenant-alias",
            "ifactory-pilot",
            "--scheduled-at",
            "2026-07-30T06:00:00Z",
            "--format",
            "json",
        ]
    )
    assert (summary.tenant_alias, summary.format) == ("ifactory-pilot", "json")


def test_scheduler_now_requires_once_and_explicit_isolated_pilot_mode(monkeypatch, capsys):
    """Allowing clock injection in production could backfill or overwrite a real slot."""
    cli = require_module("platform_integration.cli", "isolated scheduler clock gate")
    calls = []
    monkeypatch.delenv("PLATFORM_INTEGRATION_ISOLATED_PILOT_MODE", raising=False)
    monkeypatch.setattr(cli, "run_scheduler", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(
        sys,
        "argv",
        ["platform-integration", "scheduler", "--once", "--now", "2026-07-30T06:00:00Z"],
    )

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    assert exc_info.value.code == 2
    assert calls == []
    assert capsys.readouterr().err == "error: ISOLATED_PILOT_MODE_REQUIRED\n"


def test_migrate_failure_is_nonzero_and_redacted(monkeypatch, capsys):
    """Alembic failures must not disclose the database URL or migration traceback."""
    cli = require_module("platform_integration.cli", "safe migrate role")
    secret = "postgresql://user:sensitive@private-db/integration"

    def fail():
        raise RuntimeError(secret)

    monkeypatch.setattr(cli, "run_migrate", fail)
    monkeypatch.setattr(sys, "argv", ["platform-integration", "migrate"])

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    captured = capsys.readouterr()
    assert exc_info.value.code == 2
    assert captured.err == "error: SHADOW_COMMAND_FAILED\n"
    assert secret not in captured.out + captured.err
