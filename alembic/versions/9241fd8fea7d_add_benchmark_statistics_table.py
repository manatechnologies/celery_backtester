"""add_benchmark_statistics_table

Revision ID: 9241fd8fea7d
Revises: 786a79c38d0b
Create Date: 2024-06-16 12:33:12.939285

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

import uuid
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '9241fd8fea7d'
down_revision: Union[str, None] = '786a79c38d0b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'benchmark_statistics',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column('backtest_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('backtests.id'), nullable=False),
        sa.Column('total_return_percentage', sa.Float),
        sa.Column('total_return', sa.Float),
        sa.Column('max_drawdown_percent', sa.Float),
        sa.Column('max_drawdown', sa.Float),
        sa.Column('std_deviation', sa.Float),
        sa.Column('positive_periods', sa.Integer),
        sa.Column('negative_periods', sa.Integer),
        sa.Column('average_daily_return', sa.Float)
    )


def downgrade() -> None:
    op.drop_table('benchmark_statistics')
