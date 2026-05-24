"""Admin-only endpoints. Most write actions live here.

Auth: viewer/admin guards arrive in Phase 2. For now `sync` is callable
as long as the request includes the admin password header so we can
verify end-to-end during Phase 1 dev.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app.db.session import get_session
from app.settings import get_settings
from app.sync.sumo_api import SumoApiClient, sync_active_tournament

router = APIRouter(prefix="/admin", tags=["admin"])


def _require_admin(x_admin_password: str | None = Header(default=None)) -> None:
    if x_admin_password != get_settings().admin_password:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="admin only")


@router.post("/sync")
async def sync_now(
    basho_id: str,
    days: int = 15,
    session=Depends(get_session),
    _admin=Depends(_require_admin),
) -> dict:
    async with SumoApiClient() as api:
        report = await sync_active_tournament(session, api, basho_id, days=days)
    return {
        "basho_id": report.basho_id,
        "new_matches": report.new_matches,
        "days_synced": report.days_synced,
    }
