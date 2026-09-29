WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS element
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(element) OVER (PARTITION BY plan) AS plan_stddev_samp_element,
        RANK() OVER (PARTITION BY currency ORDER BY element NULLS LAST) AS element_rank,
        MAX(element) OVER (PARTITION BY plan) AS plan_max_element,
        DENSE_RANK() OVER (ORDER BY element DESC) AS element_dense_rank,
        RANK() OVER (PARTITION BY plan ORDER BY element DESC) AS element_rank_2
    FROM flattened
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        currency,
        element,
        plan_stddev_samp_element,
        element_rank,
        plan_max_element,
        element_dense_rank,
        element_rank_2,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY plan_stddev_samp_element DESC NULLS LAST) AS plan_stddev_samp_element_percent_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_stddev_samp_element DESC NULLS LAST) AS plan_stddev_samp_element_cume_dist,
        STDDEV_SAMP(plan_stddev_samp_element) OVER (PARTITION BY plan, currency) AS plan_stddev_samp_plan_stddev_samp_element
    FROM with_windows
),
enriched_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        currency,
        element,
        plan_stddev_samp_element,
        element_rank,
        plan_max_element,
        element_dense_rank,
        element_rank_2,
        plan_stddev_samp_element_percent_rank,
        plan_stddev_samp_element_cume_dist,
        plan_stddev_samp_plan_stddev_samp_element,
        PERCENT_RANK() OVER (ORDER BY plan_stddev_samp_element_percent_rank DESC) AS plan_stddev_samp_element_percent_rank_percent_rank,
        RANK() OVER (PARTITION BY currency ORDER BY element_rank DESC NULLS FIRST) AS element_rank_rank,
        DENSE_RANK() OVER (PARTITION BY currency, plan ORDER BY plan_stddev_samp_element DESC NULLS LAST) AS plan_stddev_samp_element_dense_rank
    FROM enriched
),
enriched_price_tiers_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(plan_stddev_samp_element_cume_dist) OVER (PARTITION BY currency) AS currency_avg_plan_stddev_samp_element_cume_dist,
        CUME_DIST() OVER (ORDER BY plan_stddev_samp_element_cume_dist) AS plan_stddev_samp_element_cume_dist_cume_dist,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element_dense_rank DESC) AS element_dense_rank_dense_rank
    FROM enriched_price_tiers
)
-- Window metrics over several different partitions
SELECT
    *,
    CUME_DIST() OVER w_running AS element_rank_cume_dist,
    CUME_DIST() OVER w_ordered AS element_rank_cume_dist_2
FROM enriched_price_tiers_2
WINDOW w_running AS (ORDER BY element_rank DESC), w_ordered AS (PARTITION BY plan ORDER BY element_rank NULLS LAST)
