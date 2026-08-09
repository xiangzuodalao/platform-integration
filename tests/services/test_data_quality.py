from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest


SCHEDULED_AT = datetime(2026, 7, 30, 6, 0, tzinfo=UTC)
WINDOW_START_MS = int((SCHEDULED_AT - timedelta(minutes=66)).timestamp() * 1000)


def binding(**overrides):
    values = {
        "unit": "mm/s",
        "value_scale": 2,
        "request_window_points": 66,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def point(index: int, value: str = "4.00", unit: str = "mm/s"):
    from platform_integration.services.data_quality import TelemetryPoint

    return TelemetryPoint(
        timestamp=WINDOW_START_MS + index * 60_000,
        value=value,
        unit=unit,
    )


def evaluate(points):
    from platform_integration.services.data_quality import DataQualityEvaluator

    return DataQualityEvaluator().evaluate(binding(), points, scheduled_at=SCHEDULED_AT)


def test_quality_accepts_60_to_66_distinct_finite_buckets_and_duplicate_raw_rows():
    """Counting raw rows instead of timestamp buckets would hide excessive missing history."""
    missing = {5, 15, 25, 35, 45, 55}
    result = evaluate(
        [point(index) for index in range(66) if index not in missing] + [point(0, "6.00")]
    )

    assert result.ok
    assert result.code == "OK"
    assert result.summary == {
        "distinct_bucket_count": 60,
        "missing_bucket_count": 6,
        "raw_record_count": 61,
    }


@pytest.mark.parametrize(
    ("points", "expected_code"),
    [
        ([point(index) for index in range(59)], "MISSING_RATIO_EXCEEDED"),
        (
            [point(index) for index in range(66) if index not in {10, 11, 12}],
            "CONSECUTIVE_MISSING_EXCEEDED",
        ),
        ([point(index) for index in range(66)] + [point(66)], "FUTURE_TIMESTAMP"),
        ([point(index) for index in range(66)] + [point(0, "NaN")], "NON_FINITE_VALUE"),
        ([point(index) for index in range(66)] + [point(0, "1e1")], "NON_FINITE_VALUE"),
        ([point(index) for index in range(66)] + [point(0, "4.00", "m/s")], "UNIT_MISMATCH"),
        (
            [point(index) for index in range(66)]
            + [
                point(0).__class__(
                    timestamp=point(0).timestamp + 1,
                    value="4.00",
                    unit="mm/s",
                )
            ],
            "TIMESTAMP_OUTSIDE_WINDOW",
        ),
    ],
)
def test_quality_rejects_each_invalid_history_shape_with_a_stable_code(points, expected_code):
    """Relaxing a quality branch could send invalid or future input to PDM."""
    result = evaluate(points)

    assert not result.ok
    assert result.code == expected_code


def test_quality_canonicalizes_decimal_to_the_binding_scale():
    """Using provider spelling instead of the configured scale destabilizes request identity."""
    result = evaluate([point(index, "4.005") for index in range(66)])

    assert result.ok
    assert result.points[0].value == "4.00"
    assert all(isinstance(Decimal(item.value), Decimal) for item in result.points)


def test_quality_requires_the_fixed_66_bucket_pilot_contract():
    """Accepting another window length would diverge from the fixed PDM v2 model profile."""
    from platform_integration.services.data_quality import DataQualityEvaluator

    result = DataQualityEvaluator().evaluate(
        binding(request_window_points=65),
        [point(index) for index in range(65)],
        scheduled_at=SCHEDULED_AT,
    )

    assert not result.ok
    assert result.code == "WINDOW_POINT_COUNT_MISMATCH"
