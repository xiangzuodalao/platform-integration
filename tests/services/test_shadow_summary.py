import json
from datetime import UTC, datetime
from uuid import UUID

import pytest


SLOT = datetime(2026, 7, 30, 6, 0, tzinfo=UTC)


def run(index: int, status: str = "SUCCEEDED"):
    return {
        "equipment_id": UUID(f"00000000-0000-4000-8000-{index:012d}"),
        "meas_code": f"measurement-{index:02d}",
        "tb_device_id": UUID(f"10000000-0000-4000-8000-{index:012d}"),
        "cmms_asset_id": 1000 + index,
        "model_profile_id": f"profile-{index % 6}",
        "model_artifact_sha256": f"{index % 6:x}" * 64,
        "status": status,
    }


def test_shadow_summary_is_sorted_bounded_canonical_and_contains_no_sensitive_payload():
    """Leaking raw request/forecast/config fields would violate the read-only evidence boundary."""
    from platform_integration.services.shadow_summary import build_shadow_summary

    summary = build_shadow_summary(
        tenant_alias="ifactory-pilot",
        tenant_id=UUID("00000000-0000-4000-8000-000000000001"),
        scheduled_at=SLOT,
        rows=[run(index, "FAILED" if index == 3 else "SUCCEEDED") for index in range(20, 0, -1)],
    )
    encoded = json.dumps(summary, sort_keys=True, separators=(",", ":"))

    assert summary["tenant_alias"] == "ifactory-pilot"
    assert summary["scheduled_at"] == "2026-07-30T06:00:00Z"
    assert summary["status_counts"] == {"FAILED": 1, "SUCCEEDED": 19}
    assert len(summary["mappings"]) == 20
    assert summary["mappings"] == sorted(
        summary["mappings"],
        key=lambda item: (item["equipment_id"], item["meas_code"]),
    )
    assert len(summary["models"]) == 6
    for forbidden in ("history", "forecast", "credential", "database_url", "telemetry"):
        assert forbidden not in encoded.lower()


def test_shadow_summary_rejects_more_than_100_rows_instead_of_truncating():
    """Silent truncation could make a phase-gate summary falsely look complete."""
    from platform_integration.services.shadow_summary import (
        ShadowSummaryError,
        build_shadow_summary,
    )

    with pytest.raises(ShadowSummaryError, match="SHADOW_SUMMARY_LIMIT_EXCEEDED"):
        build_shadow_summary(
            tenant_alias="ifactory-pilot",
            tenant_id=UUID("00000000-0000-4000-8000-000000000001"),
            scheduled_at=SLOT,
            rows=[run(index) for index in range(101)],
        )


def test_shadow_summary_rejects_a_non_slot_timestamp():
    """A fuzzy requested timestamp could mix evidence from different scheduler slots."""
    from platform_integration.services.shadow_summary import (
        ShadowSummaryError,
        build_shadow_summary,
    )

    with pytest.raises(ShadowSummaryError, match="SHADOW_SUMMARY_SLOT_INVALID"):
        build_shadow_summary(
            tenant_alias="ifactory-pilot",
            tenant_id=UUID("00000000-0000-4000-8000-000000000001"),
            scheduled_at=SLOT.replace(minute=1),
            rows=[],
        )
