WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
)
-- Ranking and gap features
SELECT
    *,
    CUME_DIST() OVER (PARTITION BY plan ORDER BY val ASC) AS val_cume_dist,
    STDDEV_SAMP(val) OVER (PARTITION BY plan) AS plan_stddev_samp_val,
    PERCENT_RANK() OVER (PARTITION BY plan ORDER BY val ASC) AS val_percent_rank
FROM flattened
