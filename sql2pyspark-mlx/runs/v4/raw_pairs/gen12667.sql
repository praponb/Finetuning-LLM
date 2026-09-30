WITH lines AS (
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS tier_value
),
valid AS (
    SELECT
        *,
        CAST(tier_value AS INT) AS tier_value_int,
        FLOOR(tier_value) AS tier_value_whole
    FROM lines
    WHERE tier_value <= 71
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value_whole NULLS FIRST) AS tier_value_whole_cume_dist,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value_int DESC) AS tier_value_int_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_dense_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value_int DESC NULLS FIRST) AS tier_value_int_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value_int) AS tier_value_int_percent_rank_2
    FROM valid
)
SELECT
    CASE WHEN tier_value_whole >= 81 THEN 'HIGH' ELSE 'LOW' END AS tier_value_whole_tier,
    SUM(tier_value_int_percent_rank_2) FILTER (WHERE plan = 'enterprise') AS enterprise_total_tier_value_int_percent_rank_2,
    ROUND(SUM(tier_value_int_rank) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS tier_value_int_rank_per_enterprise,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows
FROM with_windows
GROUP BY CASE WHEN tier_value_whole >= 81 THEN 'HIGH' ELSE 'LOW' END
HAVING COUNT(*) FILTER (WHERE plan = 'basic') > 1;
