WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS val
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, currency
    FROM lines
    EXCEPT ALL
    SELECT id, currency
    FROM price_tiers
    WHERE plan = 'pro'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN matched_keys k ON s.id = k.id AND s.currency = k.currency
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(val) OVER (PARTITION BY plan, currency) AS plan_sum_val,
        DENSE_RANK() OVER (PARTITION BY plan, currency ORDER BY val ASC NULLS LAST) AS val_dense_rank,
        SUM(val) OVER (PARTITION BY currency) AS currency_sum_val
    FROM reconciled
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        currency,
        plan_sum_val,
        val_dense_rank,
        currency_sum_val,
        INITCAP(TRIM(plan)) AS plan_title,
        CASE WHEN val_dense_rank >= 7 THEN 'High' WHEN val_dense_rank >= 3 THEN 'Medium' ELSE 'Low' END AS val_dense_rank_tier
    FROM metrics
    WHERE currency = 'USD'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val_dense_rank ASC) AS val_dense_rank_dense_rank,
        COUNT(plan_sum_val) OVER (PARTITION BY currency, plan) AS currency_count_plan_sum_val,
        AVG(currency_sum_val) OVER w_grp AS plan_avg_currency_sum_val,
        MIN(currency_sum_val) OVER w_grp AS plan_min_currency_sum_val
    FROM valid
    WINDOW w_grp AS (PARTITION BY plan)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(val_dense_rank_dense_rank / SUM(val_dense_rank_dense_rank) OVER (PARTITION BY currency), 4) AS val_dense_rank_dense_rank_share,
        COUNT(plan_avg_currency_sum_val) OVER (PARTITION BY plan) AS plan_count_plan_avg_currency_sum_val,
        DENSE_RANK() OVER (ORDER BY val_dense_rank ASC) AS val_dense_rank_dense_rank_2,
        DENSE_RANK() OVER (ORDER BY plan_avg_currency_sum_val DESC NULLS LAST) AS plan_avg_currency_sum_val_dense_rank,
        STDDEV_SAMP(plan_min_currency_sum_val) OVER (PARTITION BY plan) AS plan_stddev_samp_plan_min_currency_sum_val
    FROM scored
)
-- Window metrics over several different partitions
SELECT
    *,
    STDDEV_SAMP(plan_stddev_samp_plan_min_currency_sum_val) OVER w_part AS currency_stddev_samp_plan_stddev_samp_plan_min_currency_sum_val,
    SUM(plan_count_plan_avg_currency_sum_val) OVER w_part AS currency_sum_plan_count_plan_avg_currency_sum_val,
    ROUND(val_dense_rank_dense_rank / SUM(val_dense_rank_dense_rank) OVER (PARTITION BY currency), 4) AS val_dense_rank_dense_rank_share_2
FROM with_windows
WINDOW w_part AS (PARTITION BY currency);
