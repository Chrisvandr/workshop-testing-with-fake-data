from datetime import date

import pandas as pd
from structlog import get_logger

from etl.apis.cbs import CbsApi
from etl.flows.base import SqlmodelLoader
from etl.flows.utils import collect_validation_errors
from models.v1.cbs_aantal_woningen import CbsAantalWoningen

logger = get_logger(__name__)


class CbsExtractor:
    def __init__(self, api: CbsApi | None = None):
        if api is None:
            api = CbsApi()
        self.api = api

    def extract(self) -> pd.DataFrame:
        """
        Create a dataframe of the amount of woningen gerealiseerd
        per year per gemeente
        """

        dfs = []

        for year in range(date.today().year - 10, date.today().year + 1):
            records = self.api.get_gerealiseerde_woningen_for_year(year)
            if records:
                df = pd.DataFrame(records)
                df = df.rename(
                    columns={"RegioS": "gm_code", "Nieuwbouw_2": "aantal_woningen"},
                )
                df["jaar"] = year
                df = df.groupby(["gm_code", "jaar"]).sum(numeric_only=True)[
                    ["aantal_woningen"]
                ]
                dfs.append(df)

        if not dfs:
            logger.warning("No data to extract for CbsAantalWoningen")
            return pd.DataFrame()

        return pd.concat(dfs).reset_index()


class CbsTransformer:
    @staticmethod
    def transform(df: pd.DataFrame) -> list[CbsAantalWoningen]:
        objects = []
        for _, row in df.iterrows():
            obj = CbsAantalWoningen(
                gm_code=row["gm_code"],
                jaar=row["jaar"],
                aantal_woningen=row["aantal_woningen"],
            )

            if collect_validation_errors(obj):
                continue

            objects.append(obj)

        return objects


def run_cbs_flow(extractor: CbsExtractor, loader: SqlmodelLoader):
    df = extractor.extract()
    objects = CbsTransformer.transform(df)
    loader.recreate_and_load([CbsAantalWoningen], objects)


if __name__ == "__main__":
    df = CbsExtractor().extract()
