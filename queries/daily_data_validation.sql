query = f"""
    WITH combined_data AS (
        {union_query}
    ),
    ranked_data AS (
    SELECT
        quote_date,
        underlying_bid_1545,
        underlying_ask_1545,
        underlying_bid_eod,
        underlying_ask_eod,
        ROW_NUMBER() OVER (
        PARTITION BY quote_date
        ORDER BY COUNT(*) DESC
        ) AS rn
    FROM combined_data
    WHERE underlying_symbol = '^SPX'
    GROUP BY 
        quote_date, 
        underlying_bid_1545,
        underlying_ask_1545,
        underlying_bid_eod,
        underlying_ask_eod
    )
    SELECT
    quote_date,
    underlying_bid_1545,
    underlying_ask_1545,
    underlying_bid_eod,
    underlying_ask_eod
    FROM ranked_data
    WHERE rn = 1
    ORDER BY quote_date ASC;
;
"""