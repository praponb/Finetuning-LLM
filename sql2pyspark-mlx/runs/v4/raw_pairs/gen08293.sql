WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        tier_limits,
        tier_prices,
        discounts,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
)
-- Multi-level aggregation
SELECT
    CASE WHEN entry_price >= 35 THEN 'LARGE' ELSE 'SMALL' END AS entry_price_tier,
    PERCENTILE_APPROX(entry_price, ARRAY(0.25, 0.5, 0.75)) AS entry_price_quartiles,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(entry_price, 0.9) AS entry_price_p90,
    COUNT(*) AS num_rows_2,
    COUNT(*) AS row_count,
    COUNT(*) AS event_count
FROM unpacked
GROUP BY CASE WHEN entry_price >= 35 THEN 'LARGE' ELSE 'SMALL' END
