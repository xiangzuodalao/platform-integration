import hashlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import rfc8785


TENANT_ID = UUID("00000000-0000-4000-8000-000000000001")
EQUIPMENT_ID = UUID("00000000-0000-4000-8000-000000000101")
TB_DEVICE_ID = UUID("00000000-0000-4000-8000-000000000201")
CORRELATION_ID = UUID("00000000-0000-4000-8000-000000000301")
SCHEDULED_AT = datetime(2026, 7, 30, 6, 0, tzinfo=UTC)
WINDOW_START_MS = int((SCHEDULED_AT - timedelta(minutes=66)).timestamp() * 1000)
WINDOW_END_MS = int(SCHEDULED_AT.timestamp() * 1000)


def binding(**overrides):
    values = dict(
        tenant_id=TENANT_ID,
        equipment_id=EQUIPMENT_ID,
        tb_device_id=TB_DEVICE_ID,
        telemetry_key="vibration",
        meas_code="vibration_rms",
        unit="mm/s",
        sampling_frequency="1min",
        request_window_points=66,
        value_scale=2,
        model_profile_id="pilot-cnc-vibration",
        model_info_id="pilot-fixture-v1-cnc-vibration",
        scheduled_at=SCHEDULED_AT,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def points():
    from platform_integration.services.data_quality import TelemetryPoint

    return [
        TelemetryPoint(
            timestamp=WINDOW_START_MS + index * 60_000,
            value="4.0",
            unit="mm/s",
        )
        for index in range(66)
    ]


def request_projection(request):
    return {
        "tenant_id": str(request.tenant_id),
        "equipment_id": str(request.equipment_id),
        "model_profile_id": request.model_profile_id,
        "model_info_id": request.model_info_id,
        "meas_code": request.meas_code,
        "unit": request.unit,
        "sampling_frequency": request.sampling_frequency,
        "window_start": request.window_start,
        "window_end": request.window_end,
        "history": sorted(
            [item.model_dump(mode="json") for item in request.history],
            key=lambda item: (item["timestamp"], item["data_id"]),
        ),
    }


def test_builder_uses_exact_half_open_66_bucket_window_and_pdm_request_digest():
    """An inclusive end or a different digest projection would make PDM reject the request."""
    from platform_integration.services.prediction_requests import PredictionRequestBuilder

    request = PredictionRequestBuilder().build(binding(), points(), CORRELATION_ID)
    expected_digest = hashlib.sha256(rfc8785.dumps(request_projection(request))).hexdigest()

    assert request.window_start == WINDOW_START_MS
    assert request.window_end == WINDOW_END_MS
    assert len(request.history) == 66
    assert request.request_digest == expected_digest
    assert request.model_dump(mode="json")["tenant_id"] == str(TENANT_ID)


def test_builder_preserves_duplicate_raw_records_and_stably_sorts_their_data_ids():
    """Aggregating duplicates before hashing would lose provider history identity."""
    from platform_integration.services.prediction_requests import PredictionRequestBuilder

    from platform_integration.services.data_quality import TelemetryPoint

    duplicate_points = points() + [
        TelemetryPoint(timestamp=WINDOW_START_MS, value="5.0", unit="mm/s")
    ]
    request = PredictionRequestBuilder().build(binding(), duplicate_points, CORRELATION_ID)

    assert len(request.history) == 67
    first_two = [item.data_id for item in request.history[:2]]
    assert first_two == sorted(first_two)
    assert first_two[0] != first_two[1]


def test_same_timestamp_and_value_duplicates_receive_stable_occurrence_data_ids():
    """Identical provider rows sharing a data ID would be rejected by PDM as duplicate identity."""
    from platform_integration.services.prediction_requests import PredictionRequestBuilder

    identical = points() + [points()[0]]
    request = PredictionRequestBuilder().build(binding(), identical, CORRELATION_ID)
    repeated = [item.data_id for item in request.history if item.timestamp == WINDOW_START_MS]

    assert len(repeated) == 2
    assert len(set(repeated)) == 2
    assert repeated == sorted(repeated)


def test_data_id_is_rfc8785_sha256_of_canonical_identity_and_decimal_scale():
    """Omitting tenant/device/key/value/unit from identity permits cross-source collisions."""
    from platform_integration.services.prediction_requests import PredictionRequestBuilder

    request = PredictionRequestBuilder().build(binding(), points(), CORRELATION_ID)
    expected = hashlib.sha256(
        rfc8785.dumps(
            {
                "tenant_id": str(TENANT_ID),
                "tb_device_id": str(TB_DEVICE_ID),
                "telemetry_key": "vibration",
                "timestamp": WINDOW_START_MS,
                "value": "4.00",
                "unit": "mm/s",
            }
        )
    ).hexdigest()

    assert request.history[0].data_id == expected
    assert request.history[0].value == "4.00"


def test_persisted_quality_summary_contains_no_raw_history():
    """Persisting telemetry under a renamed summary key would violate data minimization."""
    from platform_integration.services.prediction_requests import PredictionRequestBuilder

    prepared = PredictionRequestBuilder().prepare(binding(), points(), CORRELATION_ID)

    assert prepared.quality_summary == {
        "distinct_bucket_count": 66,
        "missing_bucket_count": 0,
        "raw_record_count": 66,
    }
    assert "history" not in repr(prepared.quality_summary).lower()


def test_builder_uses_the_canonical_trimmed_unit_in_request_and_digest():
    """Hashing an unnormalized binding unit would disagree with PDM canonicalization."""
    from platform_integration.services.prediction_requests import PredictionRequestBuilder

    request = PredictionRequestBuilder().build(
        binding(unit=" mm/s "),
        points(),
        CORRELATION_ID,
    )

    assert request.unit == "mm/s"
    assert {item.unit for item in request.history} == {"mm/s"}
