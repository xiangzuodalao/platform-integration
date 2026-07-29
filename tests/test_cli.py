import importlib
import sys
from pathlib import Path


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
