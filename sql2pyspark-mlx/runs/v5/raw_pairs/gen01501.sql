WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        RANK() OVER (PARTITION BY plan ORDER BY element NULLS LAST) AS element_rank,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS LAST) AS element_percent_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY element DESC) AS element_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element DESC NULLS LAST) AS element_percent_rank_2,
        STDDEV_SAMP(element) OVER (PARTITION BY currency) AS currency_stddev_samp_element
    FROM lines
),
base AS (
    -- Keep recent, valid rows
    SELECT *, LEAST(element_percent_rank, currency_stddev_samp_element) AS least_element_percent_rank_currency_stddev_samp_element
    FROM metrics
    WHERE plan IN ('starter', 'enterprise')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        element_rank,
        element_cume_dist,
        currency_stddev_samp_element,
        least_element_percent_rank_currency_stddev_samp_element,
        CASE WHEN plan = 'pro' THEN 1 ELSE 0 END AS is_pro,
        CASE WHEN currency_stddev_samp_element >= 87 THEN 'High' WHEN currency_stddev_samp_element >= 11 THEN 'Medium' ELSE 'Low' END AS currency_stddev_samp_element_level
    FROM base
    WHERE plan = 'enterprise'
),
base_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        element_cume_dist,
        currency_stddev_samp_element,
        least_element_percent_rank_currency_stddev_samp_element,
        is_pro,
        currency_stddev_samp_element_level,
        element_rank + least_element_percent_rank_currency_stddev_samp_element AS element_rank_plus_least_element_percent_rank_currency_stddev_samp_element,
        TRIM(plan) AS clean_plan
    FROM valid
    WHERE element_rank <= 3
)
-- Grouped metrics
SELECT
    CASE WHEN element_cume_dist >= 0.81 THEN 'HIGH' ELSE 'LOW' END AS element_cume_dist_tier,
    plan,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(least_element_percent_rank_currency_stddev_samp_element) AS avg_least_element_percent_rank_currency_stddev_samp_element,
    ROUND(AVG(is_pro), 2) AS avg_is_pro,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN element_cume_dist >= 0.81 THEN 'HIGH' ELSE 'LOW' END ORDER BY AVG(least_element_percent_rank_currency_stddev_samp_element) DESC) AS avg_least_element_percent_rank_currency_stddev_samp_element_dense_rank
FROM base_price_tiers
GROUP BY CASE WHEN element_cume_dist >= 0.81 THEN 'HIGH' ELSE 'LOW' END, plan
HAVING basic_count > 1
ORDER BY plan DESC NULLS LAST
