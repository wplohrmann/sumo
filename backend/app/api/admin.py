"""Admin-only endpoints. Most write actions live here.

The sync endpoint pulls the active basho's Makuuchi schedule from
sumo-api.com on demand. There's no scheduler — admin clicks it.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from app.auth.deps import current_admin
from app.db.models import AppUser
from app.db.session import get_session
from app.sync.sumo_api import SumoApiClient, sync_active_tournament

router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/sync")
async def sync_now(
    basho_id: str,
    days: int = 15,
    session=Depends(get_session),
    _admin: AppUser = Depends(current_admin),
) -> dict:
    async with SumoApiClient() as api:
        report = await sync_active_tournament(session, api, basho_id, days=days)
    return {
        "basho_id": report.basho_id,
        "new_matches": report.new_matches,
        "days_synced": report.days_synced,
    }
