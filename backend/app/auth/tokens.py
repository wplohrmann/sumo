"""Token helpers.

Bearer tokens are stored in the DB as sha256 hashes. The admin uses the
configured admin password as their token; viewers get a random 32-char
secret created at provisioning time and returned to the admin once.
"""
from __future__ import annotations

import hashlib
import secrets


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def new_viewer_token() -> str:
    return secrets.token_urlsafe(24)
