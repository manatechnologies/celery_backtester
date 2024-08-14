query = f"""
WITH combined_data AS (
    {union_query}
)
SELECT
    quote_date,
    AVG(underlying_bid_1545) AS underlying_bid_1545,
    AVG(underlying_ask_1545) AS underlying_ask_1545,
    AVG(underlying_bid_eod) AS underlying_bid_eod,
    AVG(underlying_ask_eod) AS underlying_ask_eod
FROM combined_data
WHERE underlying_symbol = '^SPX'
GROUP BY quote_date
ORDER BY quote_date ASC
;
"""