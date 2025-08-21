from datetime import date

from fastapi import APIRouter

from app.api.crud.cbs import CrudCbs
from app.api.deps import SessionDep

router = APIRouter()


@router.get("/{gm_code}/aantal-woningen")
async def get_cbs_aantal_woningen(session: SessionDep, gm_code: str) -> float:
    crud = CrudCbs(session, gm_code)

    return crud.get_aantal_woningen(date.today().year - 1)
