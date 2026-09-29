WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        currency,
        tier_limits,
        tier_prices,
        discounts,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
    WHERE plan = 'starter'
)
-- Multi-level aggregation
SELECT
    plan,
    currency,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows,
    AVG(entry_price) FILTER (WHERE currency = 'THB') AS thb_avg_entry_price,
    SUM(CASE WHEN currency = 'THB' THEN entry_price ELSE 0 END) AS thb_entry_price
FROM unpacked
GROUP BY plan, currency
ORDER BY plan ASC NULLS FIRST
