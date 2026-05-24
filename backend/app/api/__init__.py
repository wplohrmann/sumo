from fastapi import APIRouter

from app.api import (
    adjustments,
    admin,
    auth,
    awards,
    me,
    picks,
    rikishi,
    standings,
    tournaments,
)

router = APIRouter()
router.include_router(auth.router)
router.include_router(me.router)
router.include_router(tournaments.router)
router.include_router(rikishi.router)
router.include_router(picks.router)
router.include_router(standings.router)
router.include_router(adjustments.router)
router.include_router(awards.router)
router.include_router(admin.router)
