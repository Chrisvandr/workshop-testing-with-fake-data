from datetime import date
from http import HTTPStatus
from unittest.mock import MagicMock

import pytest
from httpx import Request, Response

from etl.apis.cbs import CbsApi
from etl.apis.rest_client import SyncRestClient


class TestCbsApi:
    @pytest.fixture(autouse=True)
    def _assign_api_to_class(self, sync_client: SyncRestClient):
        self.api = CbsApi(client=sync_client)

    def test_should_get_gerealiseerde_woningen_for_current_year(
        self, sync_client: MagicMock
    ):
        data = [
            {
                "Gebruiksfunctie": "A045364",
                "Perioden": "2024MM01",
                "RegioS": "GM1680",
                "Nieuwbouw_2": 3,
            },
            {
                "Gebruiksfunctie": "A045364",
                "Perioden": "2024MM02",
                "RegioS": "GM1680",
                "Nieuwbouw_2": 3,
            },
        ]
        sync_client.send_request.return_value = Response(
            HTTPStatus.OK,
            json={
                "odata.metadata": "...",
                "value": data,
            },
            request=Request("GET", url=""),
        )
        results = self.api.get_gerealiseerde_woningen_for_year(date.today().year)

        assert len(results) == 2
        assert results == data


@pytest.mark.integration
# @pytest.mark.skip("Used for locally calling the CBS API")
class TestCbsApiIntegration:
    def test_get_gerealiseerde_woningen_for_year_ok(self):
        api = CbsApi()
        results = api.get_gerealiseerde_woningen_for_year(date.today().year)

        assert results

    def test_get_verkoopprijzen(self):
        api = CbsApi()
        results = api.get_verkoopprijzen()

        assert results
