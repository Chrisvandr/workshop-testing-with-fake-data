from sqlmodel import Session, and_, select

from models.v1.cbs_aantal_woningen import CbsAantalWoningen


class CrudCbs:
    def __init__(self, session: Session, gm_code: str):
        self.session = session
        self.gm_code = gm_code

    def get_aantal_woningen(self, jaar: int) -> float:
        return (
            self.session.exec(
                select(CbsAantalWoningen.aantal_woningen).where(
                    and_(
                        CbsAantalWoningen.gm_code == self.gm_code,
                        CbsAantalWoningen.jaar == jaar,
                    ),
                ),
            ).one_or_none()
            or 0.0
        )
