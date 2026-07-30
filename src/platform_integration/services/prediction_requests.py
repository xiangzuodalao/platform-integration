from __future__ import annotations

import hashlib
from dataclasses import dataclass
from uuid import UUID

import rfc8785

from platform_integration.contracts.pdm import HistoryPointV2, PredictionRequestV2
from platform_integration.services.data_quality import (
    INTERVAL_MS,
    DataQualityEvaluator,
    TelemetryPoint,
    canonical_unit,
)


class DataQualityError(ValueError):
    def __init__(self, code: str, summary: dict[str, int]) -> None:
        self.code = code
        self.summary = summary
        super().__init__(code)


@dataclass(frozen=True)
class PreparedPrediction:
    request: PredictionRequestV2
    quality_summary: dict[str, int]


def _digest(value: object) -> str:
    return hashlib.sha256(rfc8785.dumps(value)).hexdigest()


class PredictionRequestBuilder:
    def __init__(self, evaluator: DataQualityEvaluator | None = None) -> None:
        self._evaluator = evaluator or DataQualityEvaluator()

    def prepare(
        self,
        binding: object,
        points: list[TelemetryPoint] | tuple[TelemetryPoint, ...],
        correlation_id: UUID,
    ) -> PreparedPrediction:
        quality = self._evaluator.evaluate(
            binding,
            points,
            scheduled_at=binding.scheduled_at,
        )
        if not quality.ok:
            raise DataQualityError(quality.code, quality.summary)
        history = self._history(binding, quality.points)
        window_end = int(binding.scheduled_at.timestamp() * 1000)
        request_values = {
            "tenant_id": str(binding.tenant_id),
            "correlation_id": str(correlation_id),
            "equipment_id": str(binding.equipment_id),
            "model_profile_id": binding.model_profile_id,
            "model_info_id": binding.model_info_id,
            "meas_code": binding.meas_code,
            "unit": canonical_unit(binding.unit),
            "sampling_frequency": binding.sampling_frequency,
            "window_start": window_end - int(binding.request_window_points) * INTERVAL_MS,
            "window_end": window_end,
            "request_digest": "0" * 64,
            "history": history,
        }
        draft = PredictionRequestV2.model_validate(request_values)
        projection = {
            "tenant_id": str(draft.tenant_id),
            "equipment_id": str(draft.equipment_id),
            "model_profile_id": draft.model_profile_id,
            "model_info_id": draft.model_info_id,
            "meas_code": draft.meas_code,
            "unit": draft.unit,
            "sampling_frequency": draft.sampling_frequency,
            "window_start": draft.window_start,
            "window_end": draft.window_end,
            "history": [item.model_dump(mode="json") for item in draft.history],
        }
        request = draft.model_copy(update={"request_digest": _digest(projection)})
        return PreparedPrediction(request=request, quality_summary=quality.summary)

    def build(
        self,
        binding: object,
        points: list[TelemetryPoint] | tuple[TelemetryPoint, ...],
        correlation_id: UUID,
    ) -> PredictionRequestV2:
        return self.prepare(binding, points, correlation_id).request

    @staticmethod
    def _history(
        binding: object,
        points: tuple[TelemetryPoint, ...],
    ) -> list[HistoryPointV2]:
        occurrences: dict[tuple[int, str, str], int] = {}
        result: list[HistoryPointV2] = []
        for point in points:
            occurrence_key = (point.timestamp, point.value, point.unit)
            occurrence = occurrences.get(occurrence_key, 0)
            occurrences[occurrence_key] = occurrence + 1
            identity = {
                "tenant_id": str(binding.tenant_id),
                "tb_device_id": str(binding.tb_device_id),
                "telemetry_key": binding.telemetry_key,
                "timestamp": point.timestamp,
                "value": point.value,
                "unit": point.unit,
            }
            if occurrence:
                identity["occurrence"] = occurrence
            result.append(
                HistoryPointV2(
                    data_id=_digest(identity),
                    timestamp=point.timestamp,
                    value=point.value,
                    unit=point.unit,
                )
            )
        return sorted(result, key=lambda item: (item.timestamp, item.data_id))
