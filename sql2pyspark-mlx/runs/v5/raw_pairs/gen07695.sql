WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) e AS element
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(element) OVER (PARTITION BY currency, plan) currency_avg_element,
        CUME_DIST() OVER w_running element_cume_dist,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element NULLS FIRST) element_cume_dist_2
    FROM flattened
    WHERE element >= 54
    WINDOW w_running AS (PARTITION BY currency, plan ORDER BY element DESC NULLS FIRST)
),
enriched AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER (PARTITION BY currency, plan ORDER BY element_cume_dist DESC NULLS FIRST) element_cume_dist_dense_rank
    FROM scored
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        element_cume_dist * element_cume_dist_2 element_cume_dist_times_element_cume_dist_2,
        ROUND(element_cume_dist_dense_rank, 2) element_cume_dist_dense_rank_rounded
    FROM enriched
    WHERE element_cume_dist_dense_rank != 5
),
scored_price_tiers AS (
    -- Running and moving aggregates
    SELECT *, STDDEV_SAMP(currency_avg_element) OVER (PARTITION BY currency) currency_stddev_samp_currency_avg_element
    FROM base
)
SELECT
    id,
    plan,
    currency,
    currency_avg_element,
    element_cume_dist,
    element_cume_dist_2,
    element_cume_dist_dense_rank,
    element_cume_dist_times_element_cume_dist_2,
    currency_stddev_samp_currency_avg_element,
    CASE WHEN plan RLIKE '^(enterprise|starter)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS plan_zone
FROM scored_price_tiers
ORDER BY plan_zone
