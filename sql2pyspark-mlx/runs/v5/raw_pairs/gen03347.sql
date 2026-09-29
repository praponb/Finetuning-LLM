WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(discounts) AS val
    FROM price_tiers
)
SELECT
    plan,
    currency,
    PERCENTILE_APPROX(val, ARRAY(0.25, 0.5, 0.75)) AS val_quartiles,
    COUNT_IF(plan = 'basic') AS basic_events,
    SUM(val) FILTER (WHERE currency = 'THB') AS thb_total_val,
    COUNT_IF(currency = 'USD') AS usd_events,
    MIN(val) AS min_val,
    PERCENTILE_APPROX(val, ARRAY(0.25, 0.5, 0.75)) AS val_quartiles_2
FROM lines
GROUP BY GROUPING SETS ((plan, currency), (plan))
ORDER BY plan DESC NULLS FIRST
