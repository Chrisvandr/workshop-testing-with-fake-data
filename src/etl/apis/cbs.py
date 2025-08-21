from etl.apis.rest_client import SyncRestClient


class CbsApi:
    """
    Centraal Bureau voor Statistiek Public API
    """

    def __init__(self, client: SyncRestClient | None = None):
        if client is None:
            client = SyncRestClient()
        self.client = client

    @property
    def headers(self) -> dict[str, str]:
        return {}

    @property
    def endpoint(self) -> str:
        return "https://opendata.cbs.nl/ODataApi/odata"

    def get_gerealiseerde_woningen_for_year(self, year: int) -> list[dict]:
        # https://opendata.cbs.nl/statline/portal.html?_la=nl&_catalog=CBS&tableId=81955NED&_theme=397
        return self.client.send_request(
            (
                f"{self.endpoint}/81955NED/TypedDataSet"
                f"?$filter=(Gebruiksfunctie eq 'A045364')"
                f" and (substringof('{year}MM',Perioden))"
                f" and (substringof('GM',RegioS))"
                f"&$select=Gebruiksfunctie, Perioden, RegioS, Nieuwbouw_2"
            ),
            retries=2,
            include_hostname=False,
        ).json()["value"]

    def get_verkoopprijzen(self) -> list[dict]:
        # https://opendata.cbs.nl/statline/portal.html?_la=nl&_catalog=CBS&tableId=85792NED&_theme=397
        return self.client.send_request(
            f"{self.endpoint}/85792NED/TypedDataSet",
            retries=2,
            include_hostname=False,
        ).json()["value"]
