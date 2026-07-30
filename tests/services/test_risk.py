from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID


SLOT = datetime(2026, 7, 30, 6, 0, tzinfo=UTC)


def binding(direction="ABOVE", threshold="7.50"):
    return SimpleNamespace(
        risk_direction=direction,
        risk_threshold=Decimal(threshold),
        value_scale=2,
        unit="mm/s",
        scheduled_at=SLOT,
        horizon_points=15,
        tenant_id=UUID("00000000-0000-4000-8000-000000000001"),
        equipment_id=UUID("00000000-0000-4000-8000-000000000101"),
        meas_code="vibration_rms",
        policy_version="pilot-policy-v1",
    )


def forecast(values):
    return [
        SimpleNamespace(
            timestamp=int((SLOT + timedelta(minutes=index)).timestamp() * 1000),
            value=str(value),
            unit="mm/s",
        )
        for index, value in enumerate(values)
    ]


def test_round_is_risky_only_for_two_consecutive_future_threshold_crossings():
    """Counting non-consecutive crossings would activate risk for isolated forecast spikes."""
    from platform_integration.services.risk import RiskEvaluator

    evaluator = RiskEvaluator()
    isolated = evaluator.evaluate(
        binding(),
        forecast(["8.00", "7.00", "8.00"] + ["7.00"] * 12),
    )
    consecutive = evaluator.evaluate(
        binding(),
        forecast(["7.00", "8.00", "8.50"] + ["7.00"] * 12),
    )

    assert not isolated.risky
    assert isolated.threshold_crossing_count == 2
    assert consecutive.risky
    assert consecutive.threshold_crossing_count == 2
    assert consecutive.forecast_min == Decimal("7.00")
    assert consecutive.forecast_max == Decimal("8.50")


def test_two_risky_rounds_activate_and_two_healthy_rounds_clear_internal_state():
    """A one-round state transition would make the shadow signal too noisy."""
    from platform_integration.services.risk import RiskState, advance_risk_state

    state = RiskState()
    first = advance_risk_state(state, risky=True, successful=True)
    second = advance_risk_state(first, risky=True, successful=True)
    one_healthy = advance_risk_state(second, risky=False, successful=True)
    two_healthy = advance_risk_state(one_healthy, risky=False, successful=True)

    assert (first.consecutive_risk_count, first.active) == (1, False)
    assert (second.consecutive_risk_count, second.active) == (2, True)
    assert (one_healthy.consecutive_healthy_count, one_healthy.active) == (1, True)
    assert (two_healthy.consecutive_healthy_count, two_healthy.active) == (2, False)


def test_failed_or_skipped_round_resets_both_counters_but_not_active_state():
    """Carrying partial streaks over an unobserved round violates consecutive-round policy."""
    from platform_integration.services.risk import RiskState, advance_risk_state

    reset = advance_risk_state(
        RiskState(consecutive_risk_count=1, consecutive_healthy_count=1, active=True),
        risky=False,
        successful=False,
    )

    assert reset.consecutive_risk_count == 0
    assert reset.consecutive_healthy_count == 0
    assert reset.active
