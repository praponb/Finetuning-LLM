WITH lines AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) t AS tier_value
)
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    PERCENTILE_APPROX(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles,
    PERCENTILE_APPROX(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles_2,
    ROUND(SUM(tier_value) / NULLIF(COUNT(CASE WHEN currency = 'EUR' THEN 1 END), 0), 2) AS tier_value_per_eur,
    GROUPING(DATE_TRUNC('WEEK', effective_from)) AS grouping_level
FROM lines
GROUP BY CUBE (DATE_TRUNC('WEEK', effective_from))
ORDER BY tier_value_per_eur ASC;
