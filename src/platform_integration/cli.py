import argparse
import re

import uvicorn

from platform_integration.app import create_app
from platform_integration.commands.provision import (
    run_provision_apply,
    run_provision_plan,
    run_provision_verify,
)


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _sha256(value: str) -> str:
    if SHA256_RE.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("lowercase SHA-256 required")
    return value


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
    return parser


def main() -> None:
    arguments = build_parser().parse_args()
    if arguments.command == "serve":
        uvicorn.run(create_app(), host=arguments.host, port=arguments.port)
    elif arguments.command == "provision-plan":
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
