import argparse

import uvicorn

from platform_integration.app import create_app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="platform-integration")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="run the HTTP service")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    return parser


def main() -> None:
    arguments = build_parser().parse_args()
    if arguments.command == "serve":
        uvicorn.run(create_app(), host=arguments.host, port=arguments.port)
