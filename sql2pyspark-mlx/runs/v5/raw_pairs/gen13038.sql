WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS tier_value
)
-- Window metrics over several different partitions
SELECT
    *,
    DENSE_RANK() OVER (ORDER BY tier_value DESC NULLS LAST) tier_value_dense_rank,
    CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value NULLS LAST) tier_value_cume_dist,
    STDDEV_SAMP(tier_value) OVER (PARTITION BY plan) plan_stddev_samp_tier_value
FROM lines;
