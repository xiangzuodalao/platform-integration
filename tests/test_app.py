import importlib
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

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


def test_closed_loop_routes_are_disabled_by_default():
    """Deploying Phase 2 without an explicit flag must never expose a write-capable workflow."""
    app_module = require_module("platform_integration.app", "closed-loop API default gate")
    alert_id = "00000000-0000-4000-8000-000000000101"

    response = TestClient(app_module.create_app()).get(
        f"/api/v1/maintenance-alerts/{alert_id}/work-order-plan",
        headers={"X-Authorization": "Bearer browser-token"},
    )

    assert response.status_code == 503
    assert response.json() == {
        "code": "CLOSED_LOOP_DISABLED",
        "message": "closed loop disabled",
    }


def test_closed_loop_preview_and_action_forward_only_explicit_bounded_values():
    """The browser gateway contract must bind the exact token, key, alert and plan digest."""
    app_module = require_module("platform_integration.app", "closed-loop action API")
    alert_id = UUID("00000000-0000-4000-8000-000000000101")
    calls = []

    class FakeService:
        async def preview(self, target, authorization):
            calls.append(("preview", target, authorization))
            return {
                "alert_id": str(target),
                "equipment_id": "00000000-0000-4000-8000-000000000201",
                "meas_code": "vibration_rms",
                "risk_state": "ACTIVE",
                "maintenance_state": "PENDING_APPROVAL",
                "expected_version": 3,
                "priority": "HIGH",
                "forecast_summary": {
                    "minimum": "4.00",
                    "maximum": "8.50",
                    "mean": "6.25",
                    "crossing_count": 2,
                },
                "threshold": {"direction": "ABOVE", "value": "7.50", "unit": "mm/s"},
                "plan_hash": "a" * 64,
            }

        async def act(self, target, authorization, *, idempotency_key, request):
            calls.append(("act", target, authorization, idempotency_key, request))
            return {
                "action_id": "00000000-0000-4000-8000-000000000301",
                "alert_id": str(target),
                "action": "CREATE_WORK_ORDER",
                "status": "ACCEPTED",
                "correlation_id": "00000000-0000-4000-8000-000000000401",
            }, False

    application = app_module.create_app()
    application.state.maintenance_service = FakeService()
    client = TestClient(application)
    headers = {
        "X-Authorization": "Bearer browser-token",
        "Idempotency-Key": f"alert-action:{alert_id}:CREATE_WORK_ORDER",
    }

    preview = client.get(
        f"/api/v1/maintenance-alerts/{alert_id}/work-order-plan",
        headers={"X-Authorization": headers["X-Authorization"]},
    )
    action = client.post(
        f"/api/v1/maintenance-alerts/{alert_id}/actions",
        headers=headers,
        json={
            "action": "CREATE_WORK_ORDER",
            "expected_version": 3,
            "confirmed_plan_hash": "a" * 64,
        },
    )

    assert preview.status_code == 200
    assert action.status_code == 202
    assert set(preview.json()) == {
        "alert_id",
        "equipment_id",
        "meas_code",
        "risk_state",
        "maintenance_state",
        "expected_version",
        "priority",
        "forecast_summary",
        "threshold",
        "plan_hash",
    }
    assert set(action.json()) == {
        "action_id",
        "alert_id",
        "action",
        "status",
        "correlation_id",
    }
    assert calls == [
        ("preview", alert_id, "Bearer browser-token"),
        (
            "act",
            alert_id,
            "Bearer browser-token",
            f"alert-action:{alert_id}:CREATE_WORK_ORDER",
            {
                "action": "CREATE_WORK_ORDER",
                "expected_version": 3,
                "confirmed_plan_hash": "a" * 64,
            },
        ),
    ]


def test_closed_loop_action_rejects_extra_or_action_inconsistent_fields_before_service():
    """Loose request parsing could smuggle CMMS fields around the confirmed plan."""
    app_module = require_module("platform_integration.app", "strict closed-loop action body")
    application = app_module.create_app()
    application.state.maintenance_service = SimpleNamespace()
    alert_id = "00000000-0000-4000-8000-000000000101"
    headers = {
        "X-Authorization": "Bearer browser-token",
        "Idempotency-Key": f"alert-action:{alert_id}:CREATE_WORK_ORDER",
    }

    response = TestClient(application).post(
        f"/api/v1/maintenance-alerts/{alert_id}/actions",
        headers=headers,
        json={
            "action": "CREATE_WORK_ORDER",
            "expected_version": 3,
            "confirmed_plan_hash": "a" * 64,
            "priority": "LOW",
        },
    )

    assert response.status_code == 400
    assert response.json() == {"code": "REQUEST_INVALID", "message": "request invalid"}


def test_generated_provider_schema_closes_preview_and_action_response_shapes():
    """The running FastAPI provider must enforce the same closed shapes consumed by the widget."""
    app_module = require_module("platform_integration.app", "closed-loop provider schema")

    schemas = app_module.create_app().openapi()["components"]["schemas"]

    preview = schemas["WorkOrderPlanResponse"]
    accepted = schemas["ActionAcceptedResponse"]
    assert preview["additionalProperties"] is False
    assert set(preview["required"]) == {
        "alert_id",
        "equipment_id",
        "meas_code",
        "risk_state",
        "maintenance_state",
        "expected_version",
        "priority",
        "forecast_summary",
        "threshold",
        "plan_hash",
    }
    assert accepted["additionalProperties"] is False
    assert set(accepted["required"]) == {
        "action_id",
        "alert_id",
        "action",
        "status",
        "correlation_id",
    }


def test_api_rejects_missing_browser_auth_and_noncanonical_alert_uuid():
    """Delegated auth and canonical path identity are required before any service lookup."""
    app_module = require_module("platform_integration.app", "closed-loop request boundary")
    application = app_module.create_app()
    application.state.maintenance_service = SimpleNamespace()
    client = TestClient(application)

    missing_auth = client.get(
        "/api/v1/maintenance-alerts/00000000-0000-4000-8000-000000000101/work-order-plan"
    )
    uppercase = client.get(
        "/api/v1/maintenance-alerts/00000000-0000-4000-8000-000000000A01/work-order-plan",
        headers={"X-Authorization": "Bearer browser-token"},
    )

    assert missing_auth.status_code == 401
    assert missing_auth.json()["code"] == "THINGSBOARD_BROWSER_AUTH_REQUIRED"
    assert uppercase.status_code == 400
    assert uppercase.json() == {"code": "REQUEST_INVALID", "message": "request invalid"}
