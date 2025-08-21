from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import cbs_aantal_woningen, heartbeat
from app.constants import CBS, HEARTBEAT
from shared.settings import settings


def create_app() -> FastAPI:
    app = FastAPI(root_path=settings.API_STRING, openapi_url="/openapi.json")

    app.title = settings.PROJECT_NAME

    app.include_router(heartbeat.router, prefix=f"/{HEARTBEAT}")
    app.include_router(cbs_aantal_woningen.router, prefix=f"/{CBS}")

    origins = (
        [
            "*",
        ]
        if settings.LOCAL_DEVELOPMENT
        else []
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return app
