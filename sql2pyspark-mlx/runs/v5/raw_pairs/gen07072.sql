WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) e AS val
)
-- Aggregate with conditional metrics
SELECT
    currency,
    COUNT(*) AS cnt,
    SUM(CASE WHEN currency = 'USD' THEN val ELSE 0 END) AS usd_val,
    COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count
FROM lines
GROUP BY currency
ORDER BY eur_count DESC;
