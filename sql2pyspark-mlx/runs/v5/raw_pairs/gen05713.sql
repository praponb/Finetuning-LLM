WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) e AS val
)
-- Aggregate with conditional metrics
SELECT
    currency,
    ROUND(AVG(val), 2) AS avg_val,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    ROUND(SUM(val) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS val_per_usd,
    SUM(val) AS total_val,
    COUNT(CASE WHEN currency = 'THB' THEN 1 END) AS thb_count,
    SUM(val) AS total_val_2
FROM lines
GROUP BY 1
HAVING num_currencies > 1
ORDER BY val_per_usd DESC
