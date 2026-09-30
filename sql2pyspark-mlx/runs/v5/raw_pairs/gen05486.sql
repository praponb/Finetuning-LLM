WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_prices,
        discounts,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
    WHERE currency IN ('THB', 'USD')
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    CASE WHEN lowest_price >= 26 THEN 'PREMIUM' ELSE 'STANDARD' END AS lowest_price_tier,
    SUM(CASE WHEN currency = 'THB' THEN entry_price ELSE 0 END) AS thb_entry_price,
    ROUND(AVG(entry_price), 2) AS avg_entry_price
FROM flattened_attrs
GROUP BY CUBE (DATE_TRUNC('MONTH', effective_from), CASE WHEN lowest_price >= 26 THEN 'PREMIUM' ELSE 'STANDARD' END);
