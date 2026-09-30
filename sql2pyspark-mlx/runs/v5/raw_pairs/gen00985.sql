WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) e AS element
),
metrics AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER (ORDER BY element DESC NULLS LAST) AS element_dense_rank
    FROM lines
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        currency,
        element,
        element_dense_rank,
        AVG(element_dense_rank) OVER (PARTITION BY currency) AS currency_avg_element_dense_rank,
        AVG(element_dense_rank) OVER (PARTITION BY currency) AS currency_avg_element_dense_rank_2,
        DENSE_RANK() OVER (ORDER BY element DESC NULLS FIRST) AS element_dense_rank_2
    FROM metrics
),
scored AS (
    -- Ranking and gap features
    SELECT
        element,
        element_dense_rank,
        element_dense_rank_2,
        DENSE_RANK() OVER (ORDER BY element DESC) AS element_dense_rank_3,
        COUNT(currency_avg_element_dense_rank_2) OVER (PARTITION BY currency) AS currency_count_currency_avg_element_dense_rank_2
    FROM metrics_price_tiers
)
-- Running and moving aggregates
SELECT
    element_dense_rank,
    element_dense_rank_2,
    element_dense_rank_3,
    CUME_DIST() OVER (ORDER BY element_dense_rank_3 DESC NULLS FIRST) AS element_dense_rank_3_cume_dist
FROM scored
