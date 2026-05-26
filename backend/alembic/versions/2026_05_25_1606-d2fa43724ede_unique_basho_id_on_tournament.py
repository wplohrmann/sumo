"""unique basho_id on tournament

Revision ID: d2fa43724ede
Revises: 46dba3be7663
Create Date: 2026-05-25 16:06:05.864571

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'd2fa43724ede'
down_revision: Union[str, None] = '46dba3be7663'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_tournament_basho_id", "tournament", ["basho_id"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_tournament_basho_id", "tournament", type_="unique")
