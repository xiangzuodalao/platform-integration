from fastapi import FastAPI

from platform_integration.config import Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    application = FastAPI(title="iFactory Platform Integration", version="0.1.0")
    application.state.settings = settings or Settings()

    @application.get("/healthz", include_in_schema=False)
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return application


app = create_app()
