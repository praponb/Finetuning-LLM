WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
)
SELECT
    currency,
    CASE WHEN val >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_tier,
    plan,
    COUNT_IF(currency = 'USD') AS usd_events,
    APPROX_PERCENTILE(val, 0.95) AS val_p95,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    ROUND(SUM(val) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS val_per_starter,
    APPROX_PERCENTILE(val, 0.9) AS val_p90,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
    GROUPING_ID() AS grouping_id
FROM exploded
GROUP BY CUBE (currency, CASE WHEN val >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END, plan)
HAVING val_p90 >= 0
ORDER BY usd_events DESC
