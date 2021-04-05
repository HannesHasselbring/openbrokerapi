import base64
import logging
from unittest.mock import Mock

from fastapi import FastAPI, status
from fastapi.exceptions import RequestValidationError
from pytest import fixture
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.testclient import TestClient

from openbrokerapi_v2 import constants
from openbrokerapi_v2.api import get_router, BrokerCredentials
from openbrokerapi_v2.catalog import ServicePlan
from openbrokerapi_v2.log_util import configure
from openbrokerapi_v2.service_broker import Service, ServiceBroker


@fixture
def demo_service() -> Service:
    return Service(
        id="s1",
        name="service_name",
        description="service_description",
        bindable=True,
        plans=[ServicePlan(id="p1", name="default", description="plan_description")],
    )


@fixture
def mock_broker() -> ServiceBroker:
    return Mock(spec=ServiceBroker)


@fixture
def client(mock_broker) -> TestClient:
    # TODO: init app properly, don't bypass/reimplement business logic for exception handling
    from openbrokerapi_v2.response import ErrorResponse

    app = FastAPI()

    @app.exception_handler(Exception)
    def error_handler(request: Request, exc: Exception):
        # logger.exception(e)
        return JSONResponse(
            content=ErrorResponse(
                description=constants.DEFAULT_EXCEPTION_ERROR_MESSAGE,
            ).dict(),
            status_code=500,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: Exception):
        return JSONResponse(
            content=ErrorResponse(
                description=f"Required parameters not provided. detail: {str(exc)}",
                error="InvalidParameters",
            ).dict(),
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    app.include_router(
        get_router(
            mock_broker, BrokerCredentials("", ""), configure(level=logging.WARN)
        )
    )

    return TestClient(app)
