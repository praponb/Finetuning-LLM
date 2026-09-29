WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
)
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    CASE WHEN tier_value >= 70 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_tier,
    ROUND(AVG(tier_value), 2) AS avg_tier_value,
    PERCENTILE_APPROX(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles,
    AVG(tier_value) FILTER (WHERE plan = 'enterprise') AS enterprise_avg_tier_value,
    COUNT(*) AS cnt
FROM flattened
GROUP BY DATE_TRUNC('QUARTER', effective_from), DATE_TRUNC('YEAR', effective_from), CASE WHEN tier_value >= 70 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY enterprise_avg_tier_value DESC NULLS FIRST;
