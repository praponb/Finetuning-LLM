WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS val
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN val >= 62 THEN 'LARGE' ELSE 'SMALL' END AS val_tier,
    plan,
    SUM(CASE WHEN plan = 'pro' THEN val ELSE 0 END) AS pro_val,
    SUM(CASE WHEN plan = 'pro' THEN val ELSE 0 END) AS pro_val_2,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(val, 0.9) AS val_p90,
    COUNT(*) AS event_count,
    RANK() OVER (PARTITION BY CASE WHEN val >= 62 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SUM(CASE WHEN plan = 'pro' THEN val ELSE 0 END) DESC) AS pro_val_rank
FROM lines
GROUP BY val_tier, plan;
