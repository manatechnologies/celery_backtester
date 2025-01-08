import uuid
from sqlalchemy import Column, Date, Enum, DateTime, String, func, Float, Integer, ForeignKey, inspect
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Backtest(Base):
    __tablename__ = 'backtests'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id = Column(String, nullable=False)
    submitted_at = Column(DateTime, server_default=func.now())
    bigquery_table = Column(String, nullable=False)
    bigquery_table_completed = Column(String, nullable=True)
    bigquery_table_raw = Column(String, nullable=True)
    start_date = Column(Date)
    end_date = Column(Date)
    spread = Column(Integer)
    initial_portfolio_value = Column(Float)
    status = Column(Enum('running', 'completed', 'error', name='status_enum'))
    strategy = Column(Enum('percent_under', 'desired_premium', name='sell_strike_method_enum'))
    strategy_unit = Column(Float)
    execution_time = Column(Float)
    error_msg = Column(String, nullable=True)
    stack_trace = Column(String, nullable=True)

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}


class Statistic(Base):
    __tablename__ = 'statistics'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_id = Column(UUID(as_uuid=True), ForeignKey('backtests.id'), nullable=False)
    total_return_percentage = Column(Float)
    total_return = Column(Float)
    max_drawdown_percent = Column(Float)
    max_drawdown = Column(Float)
    std_deviation = Column(Float)
    positive_periods = Column(Integer)
    negative_periods = Column(Integer)
    average_daily_return = Column(Float)

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}

   
class BenchmarkStatistic(Base):
    __tablename__ = 'benchmark_statistics'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_id = Column(UUID(as_uuid=True), ForeignKey('backtests.id'), nullable=False)
    bm_total_return_percentage = Column(Float)
    bm_total_return = Column(Float)
    bm_max_drawdown_percent = Column(Float)
    bm_max_drawdown = Column(Float)
    bm_std_deviation = Column(Float)
    bm_positive_periods = Column(Integer)
    bm_negative_periods = Column(Integer)
    bm_average_daily_return = Column(Float)

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}


class SpyStatistic(Base):
    __tablename__ = 'spy_statistics'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_id = Column(UUID(as_uuid=True), ForeignKey('backtests.id'), nullable=False)
    total_return_percentage = Column(Float)
    total_return = Column(Float)
    max_drawdown_percent = Column(Float)
    max_drawdown = Column(Float)
    std_deviation = Column(Float)
    positive_periods = Column(Integer)
    negative_periods = Column(Integer)
    average_daily_return = Column(Float)

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}
    

class AcwiStatistic(Base):
    __tablename__ = 'acwi_statistics'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_id = Column(UUID(as_uuid=True), ForeignKey('backtests.id'), nullable=False)
    total_return_percentage = Column(Float)
    total_return = Column(Float)
    max_drawdown_percent = Column(Float)
    max_drawdown = Column(Float)
    std_deviation = Column(Float)
    positive_periods = Column(Integer)
    negative_periods = Column(Integer)
    average_daily_return = Column(Float)

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}
    