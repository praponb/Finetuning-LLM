WITH unpacked AS (
    SELECT
        id,
        plan,
        effective_from,
        ARRAY_MIN(tier_prices) AS lowest_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
    WHERE effective_from > '2024-06-01'
)
SELECT
    *,
    LEAD(lowest_price, 1) OVER (PARTITION BY plan ORDER BY lowest_price, id) AS next_lowest_price,
    SUM(lowest_price) OVER (PARTITION BY plan ORDER BY effective_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_lowest_price,
    FIRST_VALUE(lowest_price) IGNORE NULLS OVER (PARTITION BY plan ORDER BY effective_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_lowest_price
FROM unpacked
