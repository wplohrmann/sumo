from fastapi import APIRouter

from app.api import admin, auth, me, picks, rikishi, tournaments

router = APIRouter()
router.include_router(auth.router)
router.include_router(me.router)
router.include_router(tournaments.router)
router.include_router(rikishi.router)
router.include_router(picks.router)
router.include_router(admin.router)
