import importlib
import logging
import sys
from pathlib import Path

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
    credential_sentinel = "credential-ref://cli-boundary-sentinel"
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
