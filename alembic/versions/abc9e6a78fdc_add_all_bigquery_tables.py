"""add_all_bigquery_tables

Revision ID: abc9e6a78fdc
Revises: d71b19fa0e2f
Create Date: 2024-04-14 09:22:27.412837

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'abc9e6a78fdc'
down_revision: Union[str, None] = 'd71b19fa0e2f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('backtests', sa.Column('bigquery_table_completed', sa.String(), nullable=True))
    op.add_column('backtests', sa.Column('bigquery_table_raw', sa.String(), nullable=True))

def downgrade() -> None:
    op.drop_column('backtests', 'bigquery_table_completed')
    op.drop_column('backtests', 'bigquery_table_raw')
