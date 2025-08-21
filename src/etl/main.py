import typer

from etl.flows.base import SqlmodelLoader
from etl.flows.cbs import CbsExtractor, run_cbs_flow
from shared.log import setup_structlog
from shared.settings import settings

app = typer.Typer()

setup_structlog(settings.LOG_LEVEL)


@app.command()
def cbs_gerealiseerde_woningen_v2():
    run_cbs_flow(CbsExtractor(), SqlmodelLoader.create())


if __name__ == "__main__":
    app()
