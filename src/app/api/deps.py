from typing import Annotated

from fastapi import Depends
from sqlmodel import Session

from shared.engine import get_sessions

SessionDep = Annotated[Session, Depends(get_sessions)]
