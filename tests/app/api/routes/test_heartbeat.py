from fastapi import status
from fastapi.testclient import TestClient

from app.constants import HEARTBEAT


def test_heartbeat(client: TestClient) -> None:
    response = client.get(f"/{HEARTBEAT}")

    assert response.status_code == status.HTTP_200_OK
    content = response.json()

    assert content["status"] == "ok"
