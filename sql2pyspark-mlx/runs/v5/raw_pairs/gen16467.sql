WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS FIRST) val_percent_rank,
        DENSE_RANK() OVER (PARTITION BY plan, currency ORDER BY val DESC) val_dense_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val DESC) val_dense_rank_2
    FROM exploded
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        currency,
        val,
        val_percent_rank,
        ROUND(val_dense_rank_2 / SUM(val_dense_rank_2) OVER (PARTITION BY plan), 4) val_dense_rank_2_share
    FROM metrics
    WHERE val_percent_rank BETWEEN 0.35 AND 0.49
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        currency,
        val,
        val_dense_rank_2_share,
        MAX(val_dense_rank_2_share) OVER w_hist plan_max_val_dense_rank_2_share,
        PERCENT_RANK() OVER (ORDER BY val_dense_rank_2_share ASC NULLS LAST) val_dense_rank_2_share_percent_rank,
        CUME_DIST() OVER (PARTITION BY currency, plan ORDER BY val_percent_rank ASC) val_percent_rank_cume_dist,
        STDDEV_SAMP(val_percent_rank) OVER w_hist plan_stddev_samp_val_percent_rank
    FROM metrics_price_tiers
    WINDOW w_hist AS (PARTITION BY plan)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        plan_max_val_dense_rank_2_share - plan_stddev_samp_val_percent_rank plan_max_val_dense_rank_2_share_minus_plan_stddev_samp_val_percent_rank,
        GREATEST(val_dense_rank_2_share_percent_rank, plan_max_val_dense_rank_2_share) greatest_val_dense_rank_2_share_percent_rank_plan_max_val_dense_rank_2_share
    FROM scored
    WHERE val >= 36
)
SELECT
    id,
    currency,
    val_dense_rank_2_share,
    plan_max_val_dense_rank_2_share,
    val_dense_rank_2_share_percent_rank,
    plan_stddev_samp_val_percent_rank,
    greatest_val_dense_rank_2_share_percent_rank_plan_max_val_dense_rank_2_share,
    CASE WHEN currency RLIKE '^(THB|EUR)$' THEN 'EXTENDED' WHEN currency RLIKE '^(USD)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS currency_zone
FROM base
ORDER BY greatest_val_dense_rank_2_share_percent_rank_plan_max_val_dense_rank_2_share DESC NULLS LAST
