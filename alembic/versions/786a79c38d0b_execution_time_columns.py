"""execution_time_columns

Revision ID: 786a79c38d0b
Revises: abc9e6a78fdc
Create Date: 2024-04-26 10:32:28.096596

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '786a79c38d0b'
down_revision: Union[str, None] = 'abc9e6a78fdc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('backtests', sa.Column('execution_time', sa.Float))


def downgrade() -> None:
    op.drop_column('backtests', 'execution_time')
