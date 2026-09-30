WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS element
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(plan)) AS plan_title
    FROM lines
    WHERE plan = 'starter'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY element DESC NULLS FIRST) AS element_cume_dist,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS FIRST) AS element_dense_rank,
        MAX(element) OVER (PARTITION BY plan) AS plan_max_element,
        SUM(element) OVER (PARTITION BY currency) AS currency_sum_element
    FROM valid
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY plan_max_element DESC) AS plan_max_element_percent_rank,
        CUME_DIST() OVER w_hist AS element_cume_dist_cume_dist
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY plan, currency ORDER BY element_cume_dist DESC NULLS LAST)
)
SELECT
    plan,
    element,
    element_cume_dist,
    element_dense_rank,
    element_cume_dist_cume_dist
FROM scored
