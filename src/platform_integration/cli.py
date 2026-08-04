import argparse
import re
import sys
from datetime import datetime

import uvicorn

from platform_integration.app import create_app
from platform_integration.commands.closed_loop import (
    run_closed_loop_acceptance_verify,
    run_closed_loop_summary,
    run_closed_loop_worker,
    run_provider_readiness,
)
from platform_integration.commands.provision import (
    run_provision_apply,
    run_provision_plan,
    run_provision_verify,
)
from platform_integration.commands.shadow import (
    ShadowCommandError,
    parse_rfc3339,
    run_discover_identities,
    run_migrate,
    run_prediction_worker,
    run_scheduler,
    run_shadow_summary,
)
from platform_integration.config import Settings


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _sha256(value: str) -> str:
    if SHA256_RE.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("lowercase SHA-256 required")
    return value


def _isolated_once_now(arguments) -> datetime | None:
    now = None if arguments.now is None else parse_rfc3339(arguments.now)
    if now is not None and (not arguments.once or Settings().isolated_pilot_mode is not True):
        raise ShadowCommandError("ISOLATED_PILOT_MODE_REQUIRED")
    return now


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="platform-integration")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="run the HTTP service")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    plan = commands.add_parser("provision-plan", help="persist and print a pilot plan")
    plan.add_argument("--tenant-alias", required=True)
    plan.add_argument("--actor", required=True)
    apply = commands.add_parser("provision-apply", help="apply an exactly confirmed pilot plan")
    apply.add_argument("--tenant-alias", required=True)
    apply.add_argument("--plan-hash", required=True, type=_sha256)
    apply.add_argument("--confirmed-hash", required=True, type=_sha256)
    apply.add_argument("--actor", required=True)
    verify = commands.add_parser(
        "provision-verify", help="read one exact durable provisioning receipt"
    )
    verify.add_argument("--tenant-alias", required=True)
    verify.add_argument("--plan-hash", required=True, type=_sha256)
    commands.add_parser("migrate", help="upgrade the integration database to head")
    discover = commands.add_parser("discover-identities", help="read canonical provider IDs")
    discover.add_argument("--format", choices=("env",), required=True)
    scheduler = commands.add_parser("scheduler", help="create prediction slot rows")
    scheduler.add_argument("--once", action="store_true")
    scheduler.add_argument("--now")
    worker = commands.add_parser("prediction-worker", help="process eligible prediction rows")
    worker.add_argument("--once", action="store_true")
    worker.add_argument("--now")
    summary = commands.add_parser("shadow-summary", help="print bounded shadow evidence")
    summary.add_argument("--tenant-alias", required=True)
    summary.add_argument("--scheduled-at", required=True)
    summary.add_argument("--format", choices=("json",), required=True)
    closed_loop_worker = commands.add_parser(
        "closed-loop-worker", help="deliver one bounded closed-loop role"
    )
    closed_loop_worker.add_argument(
        "--role", choices=("alarm", "work-order", "status-poll"), required=True
    )
    closed_loop_worker.add_argument("--owner", required=True)
    closed_loop_worker.add_argument("--once", action="store_true")
    closed_loop_worker.add_argument("--now")
    closed_loop_summary = commands.add_parser(
        "closed-loop-summary", help="print bounded closed-loop evidence"
    )
    closed_loop_summary.add_argument("--tenant-alias", required=True)
    closed_loop_summary.add_argument("--format", choices=("json",), required=True)
    readiness = commands.add_parser(
        "provider-readiness", help="verify one role's external provider identity"
    )
    readiness.add_argument("--role", choices=("alarm", "work-order", "status-poll"), required=True)
    acceptance = commands.add_parser(
        "closed-loop-acceptance-verify",
        help="read and verify one exact pilot closed-loop aggregate",
    )
    acceptance.add_argument("--tenant-alias", required=True)
    acceptance.add_argument(
        "--expected-stage",
        choices=("CONSISTENT", "ACTIVE", "IN_PROGRESS", "COMPLETE", "CLEARED"),
        default="CONSISTENT",
    )
    acceptance.add_argument("--format", choices=("json",), required=True)
    return parser


def main() -> None:
    arguments = build_parser().parse_args()
    if arguments.command == "serve":
        uvicorn.run(create_app(), host=arguments.host, port=arguments.port)
        return
    try:
        if arguments.command == "provision-plan":
            run_provision_plan(arguments.tenant_alias, arguments.actor)
        elif arguments.command == "provision-apply":
            run_provision_apply(
                arguments.tenant_alias,
                arguments.plan_hash,
                arguments.confirmed_hash,
                arguments.actor,
            )
        elif arguments.command == "provision-verify":
            run_provision_verify(arguments.tenant_alias, arguments.plan_hash)
        elif arguments.command == "migrate":
            run_migrate()
        elif arguments.command == "discover-identities":
            run_discover_identities()
        elif arguments.command == "scheduler":
            now = _isolated_once_now(arguments)
            run_scheduler(once=arguments.once, now=now)
        elif arguments.command == "prediction-worker":
            now = _isolated_once_now(arguments)
            run_prediction_worker(once=arguments.once, now=now)
        elif arguments.command == "shadow-summary":
            run_shadow_summary(
                arguments.tenant_alias,
                parse_rfc3339(arguments.scheduled_at),
            )
        elif arguments.command == "closed-loop-worker":
            now = _isolated_once_now(arguments)
            run_closed_loop_worker(
                role=arguments.role,
                owner=arguments.owner,
                once=arguments.once,
                now=now,
            )
        elif arguments.command == "closed-loop-summary":
            run_closed_loop_summary(arguments.tenant_alias)
        elif arguments.command == "provider-readiness":
            run_provider_readiness(arguments.role)
        elif arguments.command == "closed-loop-acceptance-verify":
            run_closed_loop_acceptance_verify(
                arguments.tenant_alias,
                arguments.expected_stage,
            )
    except Exception as exc:
        candidate = getattr(exc, "code", None)
        if type(candidate) is str and re.fullmatch(r"[A-Z][A-Z0-9_]{2,99}", candidate):
            code = candidate
        elif arguments.command.startswith("provision-"):
            code = "PROVISIONING_COMMAND_FAILED"
        else:
            code = "SHADOW_COMMAND_FAILED"
        print(f"error: {code}", file=sys.stderr)
        raise SystemExit(2) from None
