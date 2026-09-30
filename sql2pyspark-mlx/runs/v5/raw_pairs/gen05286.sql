WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        RANK() OVER (ORDER BY element NULLS LAST) AS element_rank
    FROM lines
    WHERE plan IN ('basic', 'enterprise', 'pro')
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(element_rank) OVER (PARTITION BY plan) AS plan_stddev_samp_element_rank,
        ROUND(element_rank / SUM(element_rank) OVER (PARTITION BY plan), 4) AS element_rank_share,
        RANK() OVER (PARTITION BY plan ORDER BY element_rank DESC) AS element_rank_rank
    FROM metrics
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        element_rank,
        element_rank_share,
        element_rank_rank,
        TRIM(plan) AS clean_plan
    FROM metrics_price_tiers
    WHERE element_rank_share <= 0.79
),
metrics_price_tiers_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        element_rank,
        PERCENT_RANK() OVER (ORDER BY element_rank_share DESC NULLS FIRST) AS element_rank_share_percent_rank,
        ROUND(element_rank_rank / SUM(element_rank_rank) OVER (PARTITION BY plan), 4) AS element_rank_rank_share,
        DENSE_RANK() OVER (ORDER BY element_rank_share DESC NULLS FIRST) AS element_rank_share_dense_rank
    FROM base
),
metrics_price_tiers_3 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(element_rank / SUM(element_rank) OVER (PARTITION BY plan), 4) AS element_rank_share,
        ROUND(element_rank_share_dense_rank / SUM(element_rank_share_dense_rank) OVER (PARTITION BY plan), 4) AS element_rank_share_dense_rank_share,
        RANK() OVER (ORDER BY element_rank_share_dense_rank NULLS LAST) AS element_rank_share_dense_rank_rank
    FROM metrics_price_tiers_2
)
-- Grouped metrics
SELECT
    plan,
    MAX(element_rank_rank_share) AS max_element_rank_rank_share,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    COUNT(CASE WHEN plan = 'pro' THEN 1 END) AS pro_count
FROM metrics_price_tiers_3
GROUP BY plan
ORDER BY max_element_rank_rank_share DESC NULLS FIRST
