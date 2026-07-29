import importlib
import sys
from pathlib import Path

from fastapi.testclient import TestClient


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


def test_healthz_returns_the_service_ready_payload():
    """Removing the route or changing its payload must fail this contract."""
    app_module = require_module("platform_integration.app", "GET /healthz")

    response = TestClient(app_module.create_app()).get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
