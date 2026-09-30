WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS element
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        currency,
        element,
        MAX(element) OVER w_recent AS plan_max_element,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element DESC) AS element_cume_dist,
        STDDEV_SAMP(element) OVER w_recent AS plan_stddev_samp_element,
        RANK() OVER (ORDER BY element) AS element_rank,
        STDDEV_SAMP(element) OVER (PARTITION BY plan, currency) AS plan_stddev_samp_element_2
    FROM exploded
    WINDOW w_recent AS (PARTITION BY plan)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        currency,
        element,
        plan_max_element,
        element_cume_dist,
        element_rank,
        plan_stddev_samp_element_2,
        plan_stddev_samp_element_2 * element AS plan_stddev_samp_element_2_times_element,
        element_cume_dist + plan_stddev_samp_element AS element_cume_dist_plus_plan_stddev_samp_element
    FROM enriched
    WHERE plan_stddev_samp_element_2 >= 44
)
-- Running and moving aggregates
SELECT
    *,
    COUNT(plan_stddev_samp_element_2_times_element) OVER (PARTITION BY currency) AS currency_count_plan_stddev_samp_element_2_times_element,
    MAX(plan_stddev_samp_element_2) OVER (PARTITION BY plan) AS plan_max_plan_stddev_samp_element_2
FROM filtered;
