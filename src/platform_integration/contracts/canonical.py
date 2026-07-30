from __future__ import annotations

import hashlib
from typing import Any

import rfc8785

from platform_integration.contracts.pdm import PredictionRequestV2


def prediction_request_projection(request: PredictionRequestV2) -> dict[str, Any]:
    """Build the PDM-owned replay identity projection from JSON-mode values."""
    values = request.model_dump(mode="json")
    history = sorted(
        (
            {
                "data_id": point["data_id"],
                "timestamp": point["timestamp"],
                "value": point["value"],
                "unit": point["unit"],
            }
            for point in values["history"]
        ),
        key=lambda point: (point["timestamp"], point["data_id"]),
    )
    return {
        "tenant_id": values["tenant_id"],
        "equipment_id": values["equipment_id"],
        "model_profile_id": values["model_profile_id"],
        "model_info_id": values["model_info_id"],
        "meas_code": values["meas_code"],
        "unit": values["unit"],
        "sampling_frequency": values["sampling_frequency"],
        "window_start": values["window_start"],
        "window_end": values["window_end"],
        "history": history,
    }


def calculate_request_digest(request: PredictionRequestV2) -> str:
    """Return the SHA-256 digest of the RFC 8785 canonical request projection."""
    canonical = rfc8785.dumps(prediction_request_projection(request))
    return hashlib.sha256(canonical).hexdigest()
