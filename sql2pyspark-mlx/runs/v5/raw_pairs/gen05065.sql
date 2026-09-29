WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        plan,
        currency,
        element,
        RANK() OVER (ORDER BY element ASC NULLS LAST) element_rank,
        RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS FIRST) element_rank_2,
        PERCENT_RANK() OVER (PARTITION BY currency, plan ORDER BY element DESC NULLS FIRST) element_percent_rank,
        RANK() OVER (PARTITION BY currency ORDER BY element DESC NULLS LAST) element_rank_3,
        MAX(element) OVER (PARTITION BY currency) currency_max_element
    FROM flattened
    WHERE element IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        plan,
        currency,
        element_rank,
        element_rank_2,
        PERCENT_RANK() OVER (ORDER BY currency_max_element ASC) currency_max_element_percent_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element DESC) element_dense_rank
    FROM scored
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT
        plan,
        element_rank_2,
        CUME_DIST() OVER (ORDER BY element_rank_2 ASC NULLS FIRST) element_rank_2_cume_dist,
        ROUND(element_dense_rank / SUM(element_dense_rank) OVER (PARTITION BY currency), 4) element_dense_rank_share
    FROM metrics
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        element_dense_rank_share - element_rank_2_cume_dist element_dense_rank_share_minus_element_rank_2_cume_dist,
        CASE WHEN plan IN ('starter', 'enterprise') THEN 'Group A' ELSE 'Other' END plan_group
    FROM scored_price_tiers
    WHERE element_rank_2_cume_dist <= 0.57
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        plan,
        APPROX_PERCENTILE(element_rank_2, 0.5) AS element_rank_2_p50,
        APPROX_PERCENTILE(element_rank_2, ARRAY(0.25, 0.5, 0.75)) AS element_rank_2_quartiles,
        ROUND(SUM(element_rank_2_cume_dist) / NULLIF(COUNT(CASE WHEN plan = 'pro' THEN 1 END), 0), 2) AS element_rank_2_cume_dist_per_pro,
        ROUND(SUM(element_rank_2) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS element_rank_2_per_enterprise
    FROM filtered
    GROUP BY plan
)
SELECT
    plan,
    element_rank_2_p50,
    element_rank_2_cume_dist_per_pro,
    element_rank_2_per_enterprise
FROM grouped
ORDER BY element_rank_2_per_enterprise DESC NULLS FIRST, plan ASC;
