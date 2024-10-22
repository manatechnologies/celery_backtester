"""add_error_msg_column

Revision ID: 791339a2c688
Revises: 9241fd8fea7d
Create Date: 2024-10-20 13:42:24.450520

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '791339a2c688'
down_revision: Union[str, None] = '9241fd8fea7d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.add_column('backtests', sa.Column('error_msg', sa.Text(), nullable=True))
    op.add_column('backtests', sa.Column('stack_trace', sa.Text(), nullable=True))


def downgrade():
    op.drop_column('backtests', 'stack_trace')
    op.drop_column('backtests', 'error_msg')