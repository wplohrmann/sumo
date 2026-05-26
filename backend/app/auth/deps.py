"""FastAPI dependencies for auth."""
from __future__ import annotations

from fastapi import Cookie, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.tokens import hash_token
from app.db.models import AppUser
from app.db.session import get_session
from app.settings import get_settings


async def current_user(
    sumo_session: str | None = Cookie(default=None),
    session: AsyncSession = Depends(get_session),
) -> AppUser:
    if not sumo_session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="not authenticated"
        )
    user = await session.scalar(
        select(AppUser).where(AppUser.token_hash == hash_token(sumo_session))
    )
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token"
        )
    return user


async def current_admin(user: AppUser = Depends(current_user)) -> AppUser:
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="admin only"
        )
    return user


async def ensure_admin_user(session: AsyncSession) -> AppUser:
    """Make sure exactly one admin row exists, with token = admin_password."""
    settings = get_settings()
    admin = await session.scalar(select(AppUser).where(AppUser.role == "admin"))
    desired_hash = hash_token(settings.admin_password)
    if admin is None:
        admin = AppUser(
            display_name="admin",
            role="admin",
            token_hash=desired_hash,
        )
        session.add(admin)
        await session.commit()
        await session.refresh(admin)
    elif admin.token_hash != desired_hash:
        admin.token_hash = desired_hash
        await session.commit()
        await session.refresh(admin)
    return admin
