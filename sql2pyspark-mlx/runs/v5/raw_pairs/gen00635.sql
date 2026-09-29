WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE(discounts) AS element
    FROM price_tiers
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(element, 0) element_filled,
        ROUND(element * 1.15, 2) element_adjusted
    FROM exploded
    WHERE effective_from > DATE '2024-02-08'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        element,
        element_adjusted,
        DENSE_RANK() OVER (ORDER BY element_adjusted DESC NULLS FIRST) element_adjusted_dense_rank,
        CUME_DIST() OVER (ORDER BY element_filled DESC) element_filled_cume_dist,
        PERCENT_RANK() OVER w_hist element_adjusted_percent_rank
    FROM filtered
    WHERE effective_from > '2023-04-01'
    WINDOW w_hist AS (ORDER BY element_adjusted NULLS LAST)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY element DESC NULLS FIRST) element_cume_dist,
        DENSE_RANK() OVER w_part element_adjusted_dense_rank_dense_rank
    FROM scored
    WINDOW w_part AS (ORDER BY element_adjusted_dense_rank DESC NULLS FIRST)
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT
        effective_from,
        element,
        element_adjusted_dense_rank,
        element_filled_cume_dist,
        DENSE_RANK() OVER (ORDER BY element NULLS LAST) element_dense_rank,
        DENSE_RANK() OVER (ORDER BY element_filled_cume_dist DESC NULLS LAST) element_filled_cume_dist_dense_rank
    FROM enriched
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, element_filled_cume_dist_dense_rank - element_adjusted_dense_rank element_filled_cume_dist_dense_rank_minus_element_adjusted_dense_rank
    FROM scored_price_tiers
    WHERE element_filled_cume_dist_dense_rank IS NULL
),
filtered_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        effective_from,
        element,
        element_adjusted_dense_rank,
        element_filled_cume_dist,
        element_filled_cume_dist_dense_rank,
        element_filled_cume_dist_dense_rank_minus_element_adjusted_dense_rank,
        element - element_adjusted_dense_rank element_minus_element_adjusted_dense_rank
    FROM cleaned
    WHERE effective_from >= DATE '2024-05-16'
)
-- Running and moving aggregates
SELECT
    element_adjusted_dense_rank,
    element_filled_cume_dist,
    element_filled_cume_dist_dense_rank_minus_element_adjusted_dense_rank,
    element_minus_element_adjusted_dense_rank,
    CUME_DIST() OVER (ORDER BY element_minus_element_adjusted_dense_rank) element_minus_element_adjusted_dense_rank_cume_dist
FROM filtered_price_tiers
