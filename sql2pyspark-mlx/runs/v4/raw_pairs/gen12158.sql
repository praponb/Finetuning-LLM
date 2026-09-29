WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    *,
    STDDEV_SAMP(tier_value) OVER w_recent AS plan_stddev_samp_tier_value,
    MAX(tier_value) OVER w_recent AS plan_max_tier_value,
    STDDEV_SAMP(tier_value) OVER w_recent AS plan_stddev_samp_tier_value_2,
    CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value NULLS LAST) AS tier_value_cume_dist
FROM exploded
WINDOW w_recent AS (PARTITION BY plan)
