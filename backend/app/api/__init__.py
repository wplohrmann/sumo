from fastapi import APIRouter

from app.api import admin, auth, me, tournaments

router = APIRouter()
router.include_router(auth.router)
router.include_router(me.router)
router.include_router(tournaments.router)
router.include_router(admin.router)
