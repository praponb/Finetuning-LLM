WITH flattened_attrs AS (
    SELECT
        id,
        plan,
        effective_from,
        tier_limits,
        discounts,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
    WHERE effective_from < '2025-07-24'
)
SELECT
    plan,
    MAX(entry_price) FILTER (WHERE plan = 'pro') AS pro_max_entry_price,
    ROUND(AVG(entry_price), 2) AS avg_entry_price,
    COUNT_IF(plan = 'starter') AS starter_events,
    SUM(CASE WHEN plan = 'starter' THEN entry_price ELSE 0 END) AS starter_entry_price,
    APPROX_PERCENTILE(entry_price, 0.95) AS entry_price_p95,
    ROUND(SUM(entry_price) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS entry_price_per_enterprise
FROM flattened_attrs
GROUP BY 1
HAVING starter_events >= 1
