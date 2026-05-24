from fastapi import APIRouter

from app.api import admin

router = APIRouter()
router.include_router(admin.router)
