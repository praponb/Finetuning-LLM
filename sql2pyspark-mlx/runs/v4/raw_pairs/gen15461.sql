WITH flattened AS (
    SELECT
        id,
        plan,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) e AS element
),
with_windows AS (
    SELECT
        *,
        SUM(element) OVER (PARTITION BY plan) AS plan_sum_element,
        RANK() OVER (PARTITION BY plan ORDER BY element) AS element_rank
    FROM flattened
),
metrics AS (
    SELECT
        id,
        plan,
        element,
        plan_sum_element,
        element_rank,
        COUNT(element_rank) OVER (PARTITION BY plan) AS plan_count_element_rank,
        ROUND(element_rank / SUM(element_rank) OVER (PARTITION BY plan), 4) AS element_rank_share
    FROM with_windows
),
scored AS (
    SELECT
        id,
        plan,
        element,
        plan_sum_element,
        element_rank,
        plan_count_element_rank,
        element_rank_share,
        COUNT(element_rank) OVER (PARTITION BY plan) AS plan_count_element_rank_2,
        MIN(element_rank_share) OVER (PARTITION BY plan) AS plan_min_element_rank_share
    FROM metrics
),
metrics_price_tiers AS (
    SELECT
        id,
        plan,
        element,
        plan_sum_element,
        element_rank,
        plan_count_element_rank,
        element_rank_share,
        plan_count_element_rank_2,
        plan_min_element_rank_share,
        ROUND(element_rank / SUM(element_rank) OVER (PARTITION BY plan), 4) AS element_rank_share_2,
        RANK() OVER w_recent AS plan_sum_element_rank
    FROM scored
    WINDOW w_recent AS (PARTITION BY plan ORDER BY plan_sum_element DESC NULLS LAST)
)
SELECT
    plan,
    element_rank,
    plan_count_element_rank,
    element_rank_share,
    plan_count_element_rank_2,
    plan_min_element_rank_share,
    element_rank_share_2,
    plan_sum_element_rank
FROM metrics_price_tiers
ORDER BY plan_sum_element_rank NULLS LAST;
