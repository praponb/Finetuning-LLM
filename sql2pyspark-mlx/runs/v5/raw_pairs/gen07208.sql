WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS element
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(currency) currency_upper
    FROM flattened
    WHERE plan = 'enterprise'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        currency,
        element,
        AVG(element) OVER w_ordered currency_avg_element,
        PERCENT_RANK() OVER (ORDER BY element DESC NULLS LAST) element_percent_rank,
        AVG(element) OVER w_ordered currency_avg_element_2,
        PERCENT_RANK() OVER (PARTITION BY plan, currency ORDER BY element DESC) element_percent_rank_2,
        RANK() OVER w_part element_rank
    FROM base
    WINDOW w_ordered AS (PARTITION BY currency), w_part AS (PARTITION BY plan ORDER BY element DESC NULLS LAST)
),
base_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, currency_avg_element_2 + currency_avg_element currency_avg_element_2_plus_currency_avg_element
    FROM enriched
    WHERE element_percent_rank BETWEEN 0.25 AND 0.34
),
scored AS (
    -- Running and moving aggregates
    SELECT
        plan,
        element,
        currency_avg_element,
        element_percent_rank,
        element_percent_rank_2,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY element DESC) element_percent_rank_3,
        SUM(element_rank) OVER (PARTITION BY currency, plan) currency_sum_element_rank,
        RANK() OVER (PARTITION BY plan ORDER BY currency_avg_element_2 DESC NULLS FIRST) currency_avg_element_2_rank,
        STDDEV_SAMP(currency_avg_element_2) OVER (PARTITION BY currency) currency_stddev_samp_currency_avg_element_2,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element_percent_rank NULLS LAST) element_percent_rank_dense_rank
    FROM base_price_tiers
    WHERE element <= 80
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN currency_avg_element >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END AS currency_avg_element_tier,
    ROUND(SUM(currency_sum_element_rank) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS currency_sum_element_rank_per_basic,
    SUM(CASE WHEN plan = 'enterprise' THEN element_percent_rank_2 ELSE 0 END) AS enterprise_element_percent_rank_2,
    ROUND(AVG(element_percent_rank_3), 2) AS avg_element_percent_rank_3,
    SUM(element_percent_rank) FILTER (WHERE plan = 'basic') AS basic_total_element_percent_rank,
    SUM(CASE WHEN plan = 'starter' THEN element_percent_rank_2 ELSE 0 END) AS starter_element_percent_rank_2,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list
FROM scored
GROUP BY CASE WHEN currency_avg_element >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END;
