from fastapi import APIRouter

router = APIRouter()


@router.get("")
async def get_heartbeat() -> dict:
    return {"status": "ok"}
