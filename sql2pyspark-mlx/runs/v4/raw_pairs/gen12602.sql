WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY element) AS element_dense_rank,
        RANK() OVER w_part AS element_rank,
        PERCENT_RANK() OVER (ORDER BY element DESC) AS element_percent_rank
    FROM exploded
    WINDOW w_part AS (ORDER BY element DESC NULLS FIRST)
),
metrics_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY element_rank DESC) AS element_rank_dense_rank,
        DENSE_RANK() OVER (ORDER BY element_percent_rank NULLS LAST) AS element_percent_rank_dense_rank,
        RANK() OVER (ORDER BY element_rank DESC NULLS LAST) AS element_rank_rank,
        PERCENT_RANK() OVER (ORDER BY element NULLS FIRST) AS element_percent_rank_2
    FROM metrics
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        element,
        element_dense_rank,
        element_rank,
        element_percent_rank,
        element_rank_dense_rank,
        element_percent_rank_dense_rank,
        element_rank_rank,
        element_percent_rank_2,
        PERCENT_RANK() OVER (ORDER BY element_rank_dense_rank DESC NULLS FIRST) AS element_rank_dense_rank_percent_rank,
        DENSE_RANK() OVER (ORDER BY element_percent_rank_2 DESC NULLS FIRST) AS element_percent_rank_2_dense_rank
    FROM metrics_price_tiers
)
SELECT
    element,
    element_dense_rank,
    element_rank,
    element_rank_dense_rank,
    element_percent_rank_dense_rank,
    element_rank_rank,
    element_rank_dense_rank_percent_rank
FROM with_windows
ORDER BY element_dense_rank NULLS LAST, element_rank_rank
