from collections.abc import Generator
from typing import Annotated

from fastapi import Depends
from sqlmodel import Session

from shared.engine import get_engine


def get_db() -> Generator[Session, None, None]:
    engine = get_engine()
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_db)]
