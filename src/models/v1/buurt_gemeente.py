from sqlmodel import Field, Relationship, SQLModel

from shared.constants import source


class Buurt(SQLModel, table=True):  # type: ignore
    __table_args__ = {"schema": source}

    bu_code: str = Field(primary_key=True)
    gm_code: str = Field(foreign_key=f"{source}.gemeente.gm_code", ondelete="CASCADE")
    shape_wkt: str
    gemeente: "Gemeente" = Relationship(back_populates="buurten")


class Gemeente(SQLModel, table=True):  # type: ignore
    __table_args__ = {"schema": source}

    gm_code: str = Field(primary_key=True)
    gm_naam: str
    buurten: list[Buurt] = Relationship(back_populates="gemeente", cascade_delete=True)
