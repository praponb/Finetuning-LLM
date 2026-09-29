WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    *,
    MAX(val) OVER (PARTITION BY plan) AS plan_max_val,
    RANK() OVER (PARTITION BY plan ORDER BY val) AS val_rank,
    AVG(val) OVER (PARTITION BY currency) AS currency_avg_val,
    STDDEV_SAMP(val) OVER (PARTITION BY currency) AS currency_stddev_samp_val,
    STDDEV_SAMP(val) OVER (PARTITION BY plan) AS plan_stddev_samp_val
FROM exploded;
