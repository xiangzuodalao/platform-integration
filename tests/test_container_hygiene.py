import json
import subprocess
import uuid
from pathlib import Path


COMPONENT_ROOT = Path(__file__).parents[1]


def test_build_context_excludes_local_secrets_and_cache_files():
    """Removing ignore rules or the non-root image contract must fail this build-context check."""
    env_sentinel = COMPONENT_ROOT / ".env.test-sentinel"
    cache_sentinel = COMPONENT_ROOT / ".pytest_cache" / "test-sentinel"
    identifier = uuid.uuid4().hex
    production_image_tag = f"platform-integration-hygiene-{identifier}"
    probe_image_tag = f"platform-integration-context-probe-{identifier}"
    production_container_name = f"platform-integration-hygiene-{identifier}"
    probe_container_name = f"platform-integration-context-probe-{identifier}"

    try:
        env_sentinel.write_text("must-not-enter-image", encoding="utf-8")
        cache_sentinel.parent.mkdir(parents=True, exist_ok=True)
        cache_sentinel.write_text("must-not-enter-image", encoding="utf-8")

        assert (COMPONENT_ROOT / ".gitignore").is_file(), (
            "gitignore sentinel behavior is unavailable: .gitignore has not been implemented"
        )
        assert (COMPONENT_ROOT / ".dockerignore").is_file(), (
            "Docker build-context exclusion behavior is unavailable: .dockerignore has not been implemented"
        )
        assert (COMPONENT_ROOT / "Dockerfile").is_file(), (
            "Docker build-context exclusion behavior is unavailable: Dockerfile has not been implemented"
        )

        ignored = subprocess.run(
            [
                "git",
                "check-ignore",
                "--no-index",
                ".env.test-sentinel",
                ".pytest_cache/test-sentinel",
            ],
            cwd=COMPONENT_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert ignored.returncode == 0, "gitignore must ignore both temporary sentinels"
        assert ".env.test-sentinel" in ignored.stdout
        assert ".pytest_cache/test-sentinel" in ignored.stdout

        subprocess.run(
            ["docker", "build", "--tag", production_image_tag, "."],
            cwd=COMPONENT_ROOT,
            check=True,
        )
        inspection = subprocess.run(
            [
                "docker",
                "image",
                "inspect",
                production_image_tag,
                "--format",
                "{{json .Config.Cmd}} {{.Config.User}}",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        command, user = inspection.rsplit(" ", maxsplit=1)
        assert json.loads(command) == [
            "platform-integration",
            "serve",
            "--host",
            "0.0.0.0",
            "--port",
            "8080",
        ]
        assert user == "10001:10001"

        listing = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--name",
                production_container_name,
                "--entrypoint",
                "sh",
                production_image_tag,
                "-c",
                "test ! -e /app/.env.test-sentinel && test ! -e /app/.pytest_cache/test-sentinel "
                "&& test -d /app/src && test -f /app/pyproject.toml && test -f /app/uv.lock "
                "&& id -u && id -g",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        assert listing[-2:] == ["10001", "10001"]

        subprocess.run(
            ["docker", "build", "--file", "-", "--tag", probe_image_tag, "."],
            cwd=COMPONENT_ROOT,
            input=("FROM ghcr.io/astral-sh/uv:0.11.30-python3.12-trixie-slim\nCOPY . /context\n"),
            text=True,
            check=True,
        )
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--name",
                probe_container_name,
                "--entrypoint",
                "sh",
                probe_image_tag,
                "-c",
                "test ! -e /context/.env.test-sentinel "
                "&& test ! -e /context/.pytest_cache/test-sentinel "
                "&& test -d /context/src && test -f /context/pyproject.toml "
                "&& test -f /context/uv.lock",
            ],
            check=True,
        )
    finally:
        subprocess.run(
            ["docker", "rm", "--force", production_container_name], check=False, capture_output=True
        )
        subprocess.run(
            ["docker", "rm", "--force", probe_container_name], check=False, capture_output=True
        )
        subprocess.run(
            ["docker", "image", "rm", "--force", production_image_tag],
            check=False,
            capture_output=True,
        )
        subprocess.run(
            ["docker", "image", "rm", "--force", probe_image_tag], check=False, capture_output=True
        )
        env_sentinel.unlink(missing_ok=True)
        cache_sentinel.unlink(missing_ok=True)
