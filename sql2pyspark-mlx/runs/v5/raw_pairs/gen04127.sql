WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
)
SELECT
    CASE WHEN tier_value >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_tier,
    PERCENTILE_APPROX(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles,
    MAX(tier_value) FILTER (WHERE plan = 'enterprise') AS enterprise_max_tier_value,
    COUNT(DISTINCT currency) AS distinct_currencies,
    ROUND(SUM(tier_value) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS tier_value_per_usd,
    PERCENTILE_APPROX(tier_value, 0.9) AS tier_value_p90
FROM lines
GROUP BY CASE WHEN tier_value >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING ROUND(SUM(tier_value) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) >= 0;
