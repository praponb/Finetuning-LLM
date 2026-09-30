WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        effective_from,
        discounts,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
)
-- Multi-level aggregation
SELECT
    CASE WHEN entry_price >= 36 THEN 'HIGH' ELSE 'LOW' END AS entry_price_tier,
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    COUNT(DISTINCT plan) AS distinct_plans,
    AVG(entry_price) FILTER (WHERE plan = 'starter') AS starter_avg_entry_price,
    AVG(entry_price) FILTER (WHERE plan = 'basic') AS basic_avg_entry_price,
    MIN(entry_price) AS min_entry_price,
    COUNT(*) FILTER (WHERE plan = 'pro') AS pro_rows
FROM unpacked
GROUP BY CASE WHEN entry_price >= 36 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', effective_from)
HAVING AVG(entry_price) FILTER (WHERE plan = 'starter') > 0
