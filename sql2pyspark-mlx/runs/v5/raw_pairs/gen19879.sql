WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    id,
    plan,
    MAX(val) OVER (PARTITION BY plan) AS plan_max_val,
    PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS FIRST) AS val_percent_rank,
    CUME_DIST() OVER (PARTITION BY plan, currency ORDER BY val DESC) AS val_cume_dist,
    CUME_DIST() OVER (ORDER BY val DESC NULLS LAST) AS val_cume_dist_2,
    PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val ASC NULLS LAST) AS val_percent_rank_2
FROM exploded
