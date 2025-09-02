import typer

from etl.flows.base import SqlmodelLoader
from etl.flows.cbs_aantal_woningen import (
    CbsAantalWoningenExtractor,
    run_cbs_aantal_woningen_flow,
)
from shared.engine import get_session
from shared.log import setup_structlog
from shared.settings import settings

app = typer.Typer()

setup_structlog(settings.LOG_LEVEL)


@app.command()
def cbs_gerealiseerde_woningen():
    with get_session() as session:
        run_cbs_aantal_woningen_flow(
            CbsAantalWoningenExtractor(), SqlmodelLoader(session)
        )


if __name__ == "__main__":
    app()
