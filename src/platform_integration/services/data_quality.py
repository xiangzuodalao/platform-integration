from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN


INTERVAL_MS = 60_000
DECIMAL_RE = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")


@dataclass(frozen=True)
class TelemetryPoint:
    timestamp: int
    value: str
    unit: str


@dataclass(frozen=True)
class DataQualityResult:
    ok: bool
    code: str
    points: tuple[TelemetryPoint, ...]
    summary: dict[str, int]


def canonical_unit(value: str) -> str:
    if type(value) is not str:
        raise ValueError("unit must be a string")
    result = unicodedata.normalize("NFC", value.strip())
    if not result:
        raise ValueError("unit must be non-empty")
    return result


def canonical_decimal(value: str, *, scale: int) -> str:
    if type(value) is not str or DECIMAL_RE.fullmatch(value) is None:
        raise ValueError("decimal string required")
    try:
        parsed = Decimal(value)
        if not parsed.is_finite():
            raise ValueError("finite decimal required")
        quantized = parsed.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_EVEN)
    except (InvalidOperation, ValueError):
        raise ValueError("finite decimal required") from None
    if quantized == 0:
        quantized = abs(quantized)
    return f"{quantized:.{scale}f}"


class DataQualityEvaluator:
    def evaluate(
        self,
        binding: object,
        points: list[TelemetryPoint] | tuple[TelemetryPoint, ...],
        *,
        scheduled_at: datetime,
    ) -> DataQualityResult:
        if scheduled_at.utcoffset() is None:
            raise ValueError("scheduled_at must be timezone-aware")
        window_points = int(binding.request_window_points)
        if window_points != 66:
            return self._failed("WINDOW_POINT_COUNT_MISMATCH", [], window_points)
        window_end = int(scheduled_at.timestamp() * 1000)
        window_start = window_end - window_points * INTERVAL_MS
        expected_unit = canonical_unit(binding.unit)
        canonical: list[TelemetryPoint] = []
        identities: set[tuple[int, str, str]] = set()
        for raw in points:
            if raw.timestamp >= window_end:
                return self._failed("FUTURE_TIMESTAMP", canonical, window_points)
            if raw.timestamp < window_start or raw.timestamp % INTERVAL_MS:
                return self._failed("TIMESTAMP_OUTSIDE_WINDOW", canonical, window_points)
            try:
                unit = canonical_unit(raw.unit)
            except ValueError:
                return self._failed("UNIT_MISMATCH", canonical, window_points)
            if unit != expected_unit:
                return self._failed("UNIT_MISMATCH", canonical, window_points)
            try:
                value = canonical_decimal(raw.value, scale=int(binding.value_scale))
            except ValueError:
                return self._failed("NON_FINITE_VALUE", canonical, window_points)
            identity = (raw.timestamp, value, unit)
            if identity in identities:
                return self._failed("DUPLICATE_RAW_RECORD", canonical, window_points)
            identities.add(identity)
            canonical.append(TelemetryPoint(*identity))

        occupied = {point.timestamp for point in canonical}
        missing_indexes = [
            index
            for index in range(window_points)
            if window_start + index * INTERVAL_MS not in occupied
        ]
        if len(missing_indexes) > window_points // 10:
            return self._failed("MISSING_RATIO_EXCEEDED", canonical, window_points)
        missing = set(missing_indexes)
        if any({index, index + 1, index + 2} <= missing for index in range(window_points - 2)):
            return self._failed("CONSECUTIVE_MISSING_EXCEEDED", canonical, window_points)
        ordered = tuple(sorted(canonical, key=lambda item: (item.timestamp, item.value, item.unit)))
        return DataQualityResult(
            ok=True,
            code="OK",
            points=ordered,
            summary={
                "distinct_bucket_count": len(occupied),
                "missing_bucket_count": len(missing_indexes),
                "raw_record_count": len(canonical),
            },
        )

    @staticmethod
    def _failed(
        code: str,
        points: list[TelemetryPoint],
        window_points: int,
    ) -> DataQualityResult:
        distinct = len({item.timestamp for item in points})
        return DataQualityResult(
            ok=False,
            code=code,
            points=tuple(),
            summary={
                "distinct_bucket_count": distinct,
                "missing_bucket_count": max(0, window_points - distinct),
                "raw_record_count": len(points),
            },
        )
