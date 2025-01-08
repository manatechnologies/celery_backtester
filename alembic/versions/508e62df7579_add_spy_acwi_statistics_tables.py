"""add_spy_acwi_statistics_tables

Revision ID: 508e62df7579
Revises: 791339a2c688
Create Date: 2025-01-05 09:11:34.641626

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
import uuid



# revision identifiers, used by Alembic.
revision: str = '508e62df7579'
down_revision: Union[str, None] = '791339a2c688'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'spy_statistics',
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

    op.create_table(
        'acwi_statistics',
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
    # Drop the new tables
    op.drop_table('spy_statistics')
    op.drop_table('acwi_statistics')
    