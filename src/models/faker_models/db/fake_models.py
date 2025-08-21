from datetime import date

from factory import Faker as FactoryFaker
from factory import LazyAttribute
from factory.alchemy import SQLAlchemyModelFactory
from factory.fuzzy import FuzzyFloat
from faker import Faker

from models.v1.buurt_gemeente import Buurt, Gemeente
from models.v1.cbs_aantal_woningen import CbsAantalWoningen

fake = Faker("nl_NL")


class CbsAantalWoningenFactory(SQLAlchemyModelFactory):
    class Meta:
        model = CbsAantalWoningen
        sqlalchemy_session_persistence = "commit"

    gm_code: str = FactoryFaker("word", locale="nl_NL")
    jaar: int = date.today().year - 1
    aantal_woningen: float = FuzzyFloat(0, 10000, precision=5)


class GemeenteFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Gemeente
        sqlalchemy_session_persistence = "commit"

    class Params:
        random_code = FactoryFaker("random_number", digits=5)

    gm_code = LazyAttribute(lambda x: f"GM{x.random_code}")
    gm_naam = FactoryFaker("city", locale="nl_NL")


class BuurtFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Buurt
        sqlalchemy_session_persistence = "commit"

    class Params:
        random_code = FactoryFaker("random_number", digits=5)

    bu_code = LazyAttribute(lambda x: f"BU{x.random_code}")
    gm_code = LazyAttribute(lambda x: f"GM{x.random_code}")
    shape_wkt = "POLYGON ((0 0, 0 1, 1 1, 0 0))"
