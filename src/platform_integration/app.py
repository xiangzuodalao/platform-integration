from contextlib import asynccontextmanager
from typing import Annotated, Literal
from uuid import UUID

import httpx
from fastapi import FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator

from platform_integration.clients.thingsboard import BrowserThingsBoardClient
from platform_integration.config import Settings
from platform_integration.db import create_async_sessionmaker
from platform_integration.services.maintenance_actions import (
    MaintenanceActionError,
    MaintenanceActionService,
)


def _canonical_uuid(value: object) -> object:
    if isinstance(value, UUID):
        return value
    if type(value) is not str:
        raise ValueError("canonical UUID required")
    try:
        if str(UUID(value)) != value:
            raise ValueError("canonical UUID required")
    except ValueError:
        raise ValueError("canonical UUID required") from None
    return value


CanonicalUuid = Annotated[UUID, BeforeValidator(_canonical_uuid)]


class MaintenanceActionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    action: Literal["CREATE_WORK_ORDER", "CLOSE_RISK"]
    expected_version: int = Field(ge=1)
    confirmed_plan_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    reason: str | None = Field(default=None, min_length=1, max_length=500)

    @model_validator(mode="after")
    def validate_action_fields(self):
        if self.action == "CREATE_WORK_ORDER":
            if self.confirmed_plan_hash is None or self.reason is not None:
                raise ValueError("CREATE_WORK_ORDER requires only confirmed_plan_hash")
        elif self.reason is None or self.confirmed_plan_hash is not None:
            raise ValueError("CLOSE_RISK requires only reason")
        return self


class ForecastSummaryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    minimum: str = Field(pattern=r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
    maximum: str = Field(pattern=r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
    mean: str = Field(pattern=r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
    crossing_count: int = Field(ge=0)


class ThresholdResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    direction: Literal["ABOVE", "BELOW"]
    value: str = Field(pattern=r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
    unit: str = Field(min_length=1, max_length=40)


class WorkOrderPlanResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    alert_id: UUID = Field(strict=False)
    equipment_id: UUID = Field(strict=False)
    meas_code: str = Field(min_length=1, max_length=120)
    risk_state: Literal["ACTIVE"]
    maintenance_state: Literal["PENDING_APPROVAL"]
    expected_version: int = Field(ge=1)
    priority: Literal["HIGH"]
    forecast_summary: ForecastSummaryResponse
    threshold: ThresholdResponse
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class ActionAcceptedResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    action_id: UUID = Field(strict=False)
    alert_id: UUID = Field(strict=False)
    action: Literal["CREATE_WORK_ORDER", "CLOSE_RISK"]
    status: Literal["ACCEPTED"]
    correlation_id: UUID = Field(strict=False)


@asynccontextmanager
async def _maintenance_service(application: FastAPI):
    override = getattr(application.state, "maintenance_service", None)
    if override is not None:
        yield override
        return
    settings: Settings = application.state.settings
    if not settings.closed_loop_enabled:
        raise MaintenanceActionError("CLOSED_LOOP_DISABLED", 503)
    if (
        settings.database_url is None
        or settings.tb_base_url is None
        or settings.approver_tb_user_id is None
        or settings.pilot_work_order_equipment_id is None
    ):
        raise MaintenanceActionError("CLOSED_LOOP_CONFIGURATION_INCOMPLETE", 503)
    sessions = create_async_sessionmaker(settings.database_url.get_secret_value())
    try:
        async with httpx.AsyncClient(base_url=str(settings.tb_base_url)) as tb_http:
            yield MaintenanceActionService(
                sessions=sessions,
                thingsboard=BrowserThingsBoardClient(http=tb_http),
                approver_tb_user_id=settings.approver_tb_user_id,
                pilot_work_order_equipment_id=settings.pilot_work_order_equipment_id,
            )
    finally:
        await sessions.kw["bind"].dispose()


def create_app(settings: Settings | None = None) -> FastAPI:
    application = FastAPI(title="iFactory Platform Integration", version="0.1.0")
    application.state.settings = settings or Settings()
    application.state.maintenance_service = None

    @application.exception_handler(MaintenanceActionError)
    async def maintenance_error(_: Request, exc: MaintenanceActionError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"code": exc.code, "message": _error_message(exc.code)},
        )

    @application.exception_handler(RequestValidationError)
    async def validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        missing_auth = any(
            error.get("loc") == ("header", "X-Authorization") for error in exc.errors()
        )
        code = "THINGSBOARD_BROWSER_AUTH_REQUIRED" if missing_auth else "REQUEST_INVALID"
        return JSONResponse(
            status_code=401 if missing_auth else 400,
            content={"code": code, "message": _error_message(code)},
        )

    @application.get("/healthz", include_in_schema=False)
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @application.get(
        "/api/v1/maintenance-alerts/{alert_id}/work-order-plan",
        response_model=WorkOrderPlanResponse,
    )
    async def work_order_plan(
        alert_id: CanonicalUuid,
        request: Request,
        authorization: Annotated[
            str,
            Header(alias="X-Authorization", min_length=8, max_length=8192),
        ],
    ) -> dict[str, object]:
        async with _maintenance_service(request.app) as service:
            return await service.preview(alert_id, authorization)

    @application.post(
        "/api/v1/maintenance-alerts/{alert_id}/actions",
        status_code=202,
        response_model=ActionAcceptedResponse,
    )
    async def alert_action(
        alert_id: CanonicalUuid,
        payload: MaintenanceActionRequest,
        request: Request,
        authorization: Annotated[
            str,
            Header(alias="X-Authorization", min_length=8, max_length=8192),
        ],
        idempotency_key: Annotated[
            str,
            Header(alias="Idempotency-Key", min_length=8, max_length=200),
        ],
    ) -> dict[str, object]:
        async with _maintenance_service(request.app) as service:
            result, _ = await service.act(
                alert_id,
                authorization,
                idempotency_key=idempotency_key,
                request=payload.model_dump(mode="json", exclude_none=True),
            )
            return result

    return application


app = create_app()


def _error_message(code: str) -> str:
    return code.replace("_", " ").lower()
