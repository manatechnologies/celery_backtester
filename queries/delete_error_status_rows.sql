DELETE FROM statistics 
WHERE backtest_id IN (
    SELECT id 
    FROM backtests
    WHERE status = 'error'
);

DELETE FROM backtests
WHERE status = 'error';