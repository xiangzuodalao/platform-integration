import subprocess
import time
import uuid
from pathlib import Path


COMPONENT_ROOT = Path(__file__).parents[1]


def test_runtime_image_can_migrate_temporary_postgres_to_alembic_head():
    """Copying only package source makes the deployed migrate role unusable."""
    identifier = uuid.uuid4().hex
    image = f"platform-integration-migrate-{identifier}"
    network = f"platform-integration-migrate-{identifier}"
    database = f"platform-integration-postgres-{identifier}"
    try:
        subprocess.run(["docker", "network", "create", network], check=True, capture_output=True)
        subprocess.run(
            [
                "docker",
                "run",
                "--detach",
                "--name",
                database,
                "--network",
                network,
                "--env",
                "POSTGRES_PASSWORD=test",
                "--env",
                "POSTGRES_USER=test",
                "--env",
                "POSTGRES_DB=test",
                "postgres:16-alpine",
            ],
            check=True,
            capture_output=True,
        )
        for _ in range(60):
            ready = subprocess.run(
                ["docker", "exec", database, "pg_isready", "-U", "test", "-d", "test"],
                check=False,
                capture_output=True,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise AssertionError("temporary PostgreSQL did not become ready")

        subprocess.run(
            ["docker", "build", "--tag", image, "."],
            cwd=COMPONENT_ROOT,
            check=True,
            capture_output=True,
        )
        migrated = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                network,
                "--env",
                (
                    "PLATFORM_INTEGRATION_DATABASE_URL="
                    f"postgresql+psycopg://test:test@{database}:5432/test"
                ),
                image,
                "platform-integration",
                "migrate",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        assert migrated.returncode == 0, migrated.stderr
        revision = subprocess.run(
            [
                "docker",
                "exec",
                database,
                "psql",
                "-U",
                "test",
                "-d",
                "test",
                "-Atc",
                "SELECT version_num FROM alembic_version",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        assert revision == "0003_internal_risk_state"
    finally:
        subprocess.run(["docker", "rm", "--force", database], check=False, capture_output=True)
        subprocess.run(["docker", "network", "rm", network], check=False, capture_output=True)
        subprocess.run(
            ["docker", "image", "rm", "--force", image], check=False, capture_output=True
        )
