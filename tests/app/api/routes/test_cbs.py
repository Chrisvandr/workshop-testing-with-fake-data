import pytest
from fastapi import status
from fastapi.testclient import TestClient

from app.constants import CBS
from models.faker_models.db.fake_models import CbsAantalWoningenFactory
from shared.settings import settings


class TestCbsAantalWoningen:
    def endpoint(self, gm_code: str) -> str:
        return f"{settings.API_STRING}/{CBS}/{gm_code}/aantal-woningen"

    @pytest.mark.integration
    def test_get_aantal_woningen(self, api_client: TestClient) -> None:
        # GM0202 is arnhem
        response = api_client.get(self.endpoint("GM0202"))

        assert response.status_code == status.HTTP_200_OK
        content = response.json()

        assert isinstance(content, float)

    @pytest.mark.docker
    def test_should_get_aantal_woningen_and_return_success(
        self, client: TestClient, gm_code: str
    ) -> None:
        cbs = CbsAantalWoningenFactory(gm_code=gm_code)

        response = client.get(self.endpoint(gm_code))

        assert response.status_code == status.HTTP_200_OK
        content = response.json()

        assert content == cbs.aantal_woningen
