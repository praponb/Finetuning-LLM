WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS element
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        currency,
        element,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS LAST) AS element_dense_rank,
        STDDEV_SAMP(element) OVER (PARTITION BY plan) AS plan_stddev_samp_element,
        MAX(element) OVER (PARTITION BY plan, currency) AS plan_max_element,
        RANK() OVER (ORDER BY element NULLS LAST) AS element_rank,
        PERCENT_RANK() OVER (ORDER BY element DESC NULLS FIRST) AS element_percent_rank
    FROM exploded
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY element_rank NULLS LAST) AS element_rank_percent_rank,
        DENSE_RANK() OVER w_hist AS plan_max_element_dense_rank,
        RANK() OVER w_ordered AS element_dense_rank_rank,
        RANK() OVER (PARTITION BY currency, plan ORDER BY plan_max_element DESC) AS plan_max_element_rank
    FROM scored
    WINDOW w_hist AS (PARTITION BY currency ORDER BY plan_max_element DESC NULLS FIRST), w_ordered AS (ORDER BY element_dense_rank NULLS LAST)
),
scored_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        currency,
        element,
        element_dense_rank,
        plan_stddev_samp_element,
        plan_max_element,
        element_rank,
        element_percent_rank,
        element_rank_percent_rank,
        plan_max_element_dense_rank,
        element_dense_rank_rank,
        plan_max_element_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY plan_max_element_dense_rank DESC NULLS LAST) AS plan_max_element_dense_rank_cume_dist,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element_dense_rank_rank NULLS FIRST) AS element_dense_rank_rank_dense_rank
    FROM metrics
),
metrics_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY element_percent_rank NULLS LAST) AS element_percent_rank_percent_rank,
        ROUND(element_dense_rank_rank / SUM(element_dense_rank_rank) OVER (PARTITION BY currency), 4) AS element_dense_rank_rank_share,
        DENSE_RANK() OVER (ORDER BY plan_stddev_samp_element) AS plan_stddev_samp_element_dense_rank,
        PERCENT_RANK() OVER w_grp AS plan_stddev_samp_element_percent_rank
    FROM scored_price_tiers
    WINDOW w_grp AS (PARTITION BY currency ORDER BY plan_stddev_samp_element)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(plan_stddev_samp_element AS STRING) AS plan_stddev_samp_element_num,
        ROUND(plan_stddev_samp_element_percent_rank, 1) AS plan_stddev_samp_element_percent_rank_rounded
    FROM metrics_price_tiers
    WHERE element_dense_rank_rank = 7
)
-- Multi-level aggregation
SELECT
    plan,
    ROUND(SUM(element_dense_rank_rank) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS element_dense_rank_rank_per_starter,
    ROUND(AVG(plan_stddev_samp_element), 2) AS avg_plan_stddev_samp_element,
    COUNT(*) AS row_count
FROM filtered
GROUP BY ROLLUP (plan)
