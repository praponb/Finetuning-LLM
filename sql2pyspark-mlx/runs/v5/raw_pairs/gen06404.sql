WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        effective_from,
        tier_limits,
        discounts,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
    WHERE effective_from BETWEEN '2024-06-26' AND '2025-06-01'
)
-- Window metrics over several different partitions
SELECT
    id,
    lowest_price,
    avg_tier_price,
    SUM(lowest_price) OVER (ORDER BY lowest_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_lowest_price,
    FIRST_VALUE(lowest_price, TRUE) OVER (ORDER BY lowest_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_lowest_price,
    ROW_NUMBER() OVER (ORDER BY lowest_price, id) AS event_seq,
    FIRST_VALUE(lowest_price, TRUE) OVER (ORDER BY avg_tier_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_lowest_price_2,
    NTILE(3) OVER (ORDER BY avg_tier_price, id) AS avg_tier_price_bucket
FROM unpacked
