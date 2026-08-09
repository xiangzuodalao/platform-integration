from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from platform_integration.services.data_quality import (
    INTERVAL_MS,
    canonical_decimal,
    canonical_unit,
)


@dataclass(frozen=True)
class RoundRisk:
    risky: bool
    threshold_crossing_count: int
    forecast_min: Decimal
    forecast_max: Decimal
    forecast_mean: Decimal


@dataclass(frozen=True)
class RiskState:
    consecutive_risk_count: int = 0
    consecutive_healthy_count: int = 0
    active: bool = False


def advance_risk_state(state: RiskState, *, risky: bool, successful: bool) -> RiskState:
    if not successful:
        return RiskState(active=state.active)
    if risky:
        count = state.consecutive_risk_count + 1
        return RiskState(
            consecutive_risk_count=count,
            consecutive_healthy_count=0,
            active=state.active or count >= 2,
        )
    count = state.consecutive_healthy_count + 1
    return RiskState(
        consecutive_risk_count=0,
        consecutive_healthy_count=count,
        active=state.active and count < 2,
    )


class RiskEvaluator:
    def evaluate(self, binding: object, forecast: list[object]) -> RoundRisk:
        if len(forecast) != int(binding.horizon_points):
            raise ValueError("FORECAST_POINT_COUNT_MISMATCH")
        expected_unit = canonical_unit(binding.unit)
        threshold = Decimal(
            canonical_decimal(str(binding.risk_threshold), scale=binding.value_scale)
        )
        start_ms = int(binding.scheduled_at.timestamp() * 1000)
        values: list[Decimal] = []
        crossings: list[bool] = []
        for index, point in enumerate(forecast):
            if point.timestamp != start_ms + index * INTERVAL_MS:
                raise ValueError("FORECAST_TIMESTAMP_MISMATCH")
            if canonical_unit(point.unit) != expected_unit:
                raise ValueError("FORECAST_UNIT_MISMATCH")
            value = Decimal(canonical_decimal(point.value, scale=binding.value_scale))
            values.append(value)
            crossings.append(
                value > threshold if binding.risk_direction == "ABOVE" else value < threshold
            )
        risky = any(crossings[index] and crossings[index + 1] for index in range(len(values) - 1))
        mean = sum(values) / len(values)
        scale = int(binding.value_scale)
        return RoundRisk(
            risky=risky,
            threshold_crossing_count=sum(crossings),
            forecast_min=min(values),
            forecast_max=max(values),
            forecast_mean=Decimal(canonical_decimal(format(mean, "f"), scale=scale)),
        )
