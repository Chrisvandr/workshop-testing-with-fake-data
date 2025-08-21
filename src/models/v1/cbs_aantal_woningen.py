from sqlmodel import Field, SQLModel

from shared.constants import source


class CbsAantalWoningen(SQLModel, table=True):  # type: ignore
    __tablename__ = "cbs_aantal_woningen"
    __table_args__ = {"schema": source}

    gm_code: str = Field(primary_key=True)
    jaar: int = Field(primary_key=True)
    aantal_woningen: float
