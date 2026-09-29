WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
    WHERE plan IN ('enterprise', 'pro')
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    plan,
    CASE WHEN entry_price >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END AS entry_price_tier,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows,
    SUM(CASE WHEN currency = 'USD' THEN entry_price ELSE 0 END) AS usd_entry_price,
    COUNT(*) AS num_rows,
    ROUND(SUM(entry_price) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS entry_price_per_basic,
    COUNT(CASE WHEN currency = 'THB' THEN 1 END) AS thb_count,
    SIZE(COLLECT_SET(plan)) AS num_plans
FROM flattened_attrs
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', effective_from), plan, CASE WHEN entry_price >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('WEEK', effective_from), plan), ());
