import traceback
from collections.abc import Callable
from typing import Any

import structlog
import structlog.contextvars
from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.responses import Response
from structlog import get_logger

from app.create_app import create_app
from shared.log import setup_structlog
from shared.settings import settings

setup_structlog(log_level=settings.LOG_LEVEL)
logger = get_logger(__name__)

app = create_app()


@app.exception_handler(Exception)
async def unicorn_exception_handler(request: Request, exc: Exception) -> JSONResponse:  # noqa: ARG001
    if settings.ENVIRONMENT == "prd":
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "message": "The API had an error.",
            },
        )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "message": "The API had an error.",
            "traceback": traceback.format_exception(exc),
        },
    )


@app.middleware("http")
async def add_content_security_policy_and_log_request_context_variables(
    request: Request,
    call_next: Callable,
) -> Any:
    structlog.contextvars.bind_contextvars(
        method=request.method,
        path=request.url.path + f"?{request.query_params}",
        ip=request.client.host,
    )
    return await call_next(request)


def set_csp_header_for_swagger(response: Response):
    if (
        response.headers.get("Content-Type")
        and "text/html" in response.headers["Content-Type"]
    ):
        response.headers["Content-Security-Policy"] = (
            "default-src 'self' 'unsafe-inline' "
            "http://www.w3.org/2000/svg https://cdn.jsdelivr.net/ "
            "https://fastapi.tiangolo.com/img/favicon.png; style-src 'self' "
            "'unsafe-inline' https://cdn.jsdelivr.net/; object-src 'none'; "
            "base-uri 'self'; frame-ancestors 'self'; form-action 'self';"
        )
