WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) li AS element
),
enriched AS (
    SELECT
        *,
        MIN(element) OVER (PARTITION BY currency) currency_min_element,
        RANK() OVER (PARTITION BY plan, currency ORDER BY element DESC) element_rank,
        SUM(element) OVER (PARTITION BY currency) currency_sum_element,
        AVG(element) OVER (PARTITION BY currency) currency_avg_element
    FROM exploded
),
metrics AS (
    SELECT *, SUM(currency_avg_element) OVER w_ordered plan_sum_currency_avg_element
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY plan)
),
with_windows AS (
    SELECT
        id,
        plan,
        effective_from,
        element,
        currency_sum_element,
        currency_avg_element,
        plan_sum_currency_avg_element,
        PERCENT_RANK() OVER w_running element_rank_percent_rank,
        COUNT(element_rank) OVER (PARTITION BY plan) plan_count_element_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY currency_avg_element DESC NULLS LAST) currency_avg_element_dense_rank,
        DENSE_RANK() OVER (ORDER BY currency_min_element DESC) currency_min_element_dense_rank,
        SUM(currency_min_element) OVER (PARTITION BY currency) currency_sum_currency_min_element
    FROM metrics
    WINDOW w_running AS (PARTITION BY plan ORDER BY element_rank DESC NULLS LAST)
),
with_windows_price_tiers AS (
    SELECT
        plan,
        effective_from,
        currency_sum_element,
        currency_avg_element,
        plan_sum_currency_avg_element,
        element_rank_percent_rank,
        currency_min_element_dense_rank,
        currency_sum_currency_min_element,
        ROUND(plan_count_element_rank / SUM(plan_count_element_rank) OVER (PARTITION BY plan), 4) plan_count_element_rank_share,
        COUNT(currency_avg_element) OVER (PARTITION BY plan) plan_count_currency_avg_element,
        DENSE_RANK() OVER (ORDER BY currency_sum_currency_min_element DESC NULLS FIRST) currency_sum_currency_min_element_dense_rank
    FROM with_windows
)
SELECT
    plan,
    effective_from,
    currency_sum_element,
    plan_sum_currency_avg_element,
    currency_sum_currency_min_element,
    plan_count_element_rank_share,
    plan_count_currency_avg_element,
    currency_sum_currency_min_element_dense_rank,
    CASE WHEN plan RLIKE '^(pro|basic)$' THEN 'GROUP_A' WHEN plan RLIKE '^(starter|enterprise)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS plan_zone
FROM with_windows_price_tiers
