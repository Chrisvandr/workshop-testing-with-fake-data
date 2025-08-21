from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest
from sqlmodel import Session
from structlog.testing import LogCapture

from etl.apis.cbs import CbsApi
from etl.flows.base import SqlmodelLoader
from etl.flows.cbs import CbsExtractor, run_cbs_flow
from models.faker_models.db.fake_models import GemeenteFactory
from models.v1.cbs_aantal_woningen import CbsAantalWoningen
from tests.etl.utils import get_row_count


class TestCbsExtractor:
    @pytest.fixture(autouse=True)
    def _assign_api_to_class(self):
        self.api = MagicMock(spec=CbsApi)

    def test_raises_exception_when_missing_data(self, log_output: LogCapture):
        """
        An exception should be raised when no data is returned by the cbs API
        """
        self.api.get_gerealiseerde_woningen_for_year.return_value = []

        CbsExtractor(self.api).extract()
        assert "No data to extract for" in log_output.entries[0]["event"]

    def test_missing_current_year_data(self):
        """
        When the cbs data for the current year is not available yet the
        data for the previous year should still be sucessfully retrieved
        """

        def mocked_get_gerealiseerde_woningen_for_year(year: int) -> list[dict]:
            """
            This function simulates no data for the current year being available yet
            by returning an empty list for the current year
            and a normal result for any other year
            """
            if year == date.today().year:
                return []
            return [
                {
                    "Gebruiksfunctie": "A045364",
                    "Perioden": "2024MM01",
                    "RegioS": "GM1680",
                    "Nieuwbouw_2": 3,
                },
                {
                    "Gebruiksfunctie": "A045364",
                    "Perioden": "2024MM01",
                    "RegioS": "GM1680",
                    "Nieuwbouw_2": 3,
                },
            ]

        self.api.get_gerealiseerde_woningen_for_year.side_effect = (
            mocked_get_gerealiseerde_woningen_for_year
        )

        df = CbsExtractor(self.api).extract()
        assert len(df) == 1


@pytest.mark.docker
class TestCbsFlow:
    def test_should_write_cbs_objects(
        self,
        postgres_loader: SqlmodelLoader,
        db_session: Session,
        current_year: int,
    ):
        extractor = MagicMock(spec=CbsExtractor)
        extractor.extract.return_value = pd.DataFrame(
            {
                "gm_code": [
                    "gm1",
                ],
                "jaar": [
                    current_year,
                ],
                "aantal_woningen": [
                    1,
                ],
            },
        )
        GemeenteFactory.create(gm_code="gm1")

        run_cbs_flow(extractor, postgres_loader)

        assert get_row_count(db_session, CbsAantalWoningen) == 1

    def test_should_not_write_cbs_objects_when_invalid_gm_code(
        self,
        postgres_loader: SqlmodelLoader,
        current_year: int,
        db_session: Session,
    ):
        """
        Test whether the flow keeps running even though there is an invalid gm_code
        """
        extractor = MagicMock(spec=CbsExtractor)
        extractor.extract.return_value = pd.DataFrame(
            {
                "gm_code": [
                    1,  # invalid
                ],
                "jaar": [
                    current_year,
                ],
                "aantal_woningen": [
                    1,
                ],
            },
        )
        with pytest.raises(ValueError):
            run_cbs_flow(extractor, postgres_loader)

        # There should be no records in the database, invalid gm_code
        assert get_row_count(db_session, CbsAantalWoningen) == 0
