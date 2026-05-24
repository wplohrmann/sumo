from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import current_user, ensure_admin_user
from app.auth.tokens import hash_token
from app.db.models import AppUser
from app.db.session import get_session
from app.settings import get_settings

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    token: str


class UserOut(BaseModel):
    id: str
    display_name: str
    role: str
    spoiler_day: int | None = None


def _serialize(user: AppUser) -> UserOut:
    return UserOut(
        id=str(user.id),
        display_name=user.display_name,
        role=user.role,
        spoiler_day=user.spoiler_day,
    )


@router.post("/login", response_model=UserOut)
async def login(
    payload: LoginRequest,
    response: Response,
    session: AsyncSession = Depends(get_session),
) -> UserOut:
    settings = get_settings()
    token = payload.token.strip()
    if not token:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="empty token")

    if token == settings.admin_password:
        user = await ensure_admin_user(session)
    else:
        user = await session.scalar(
            select(AppUser).where(AppUser.token_hash == hash_token(token))
        )
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token"
            )

    response.set_cookie(
        key="sumo_session",
        value=token,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="lax",
        max_age=60 * 60 * 24 * 30,
    )
    return _serialize(user)


@router.post("/logout")
async def logout(response: Response) -> dict:
    response.delete_cookie("sumo_session")
    return {"ok": True}


@router.get("/me", response_model=UserOut)
async def me(user: AppUser = Depends(current_user)) -> UserOut:
    return _serialize(user)
