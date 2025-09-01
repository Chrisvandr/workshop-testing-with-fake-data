import pytest
from sqlmodel import Session

from app.api.routes.cbs_aantal_woningen import CrudCbs
from models.faker_models.db.fake_models import CbsAantalWoningenFactory


@pytest.mark.docker
def test_get_aantal_woningen(db_session: Session, gm_code: str):
    cbs = CbsAantalWoningenFactory(gm_code=gm_code)

    assert (
        CrudCbs(db_session, gm_code).get_aantal_woningen(jaar=cbs.jaar)
        == cbs.aantal_woningen
    )



@pytest.mark.docker
def test_get_aantal_woningen_alternative_version(db_session: Session, gm_code: str):
    """This test may not be a great idea"""
    cbs = CbsAantalWoningenFactory(gm_code=gm_code)

    assert (
        CrudCbs(db_session, gm_code).get_aantal_woningen(jaar=cbs.jaar)
        == 9749.4
    )
