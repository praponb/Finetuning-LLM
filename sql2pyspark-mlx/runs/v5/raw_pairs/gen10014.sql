WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS element
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY element NULLS FIRST) AS element_rank,
        DENSE_RANK() OVER w_hist AS element_dense_rank,
        PERCENT_RANK() OVER (ORDER BY element DESC NULLS LAST) AS element_percent_rank
    FROM exploded
    WHERE effective_from > '2024-10-01'
    WINDOW w_hist AS (ORDER BY element DESC NULLS FIRST)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, DENSE_RANK() OVER w_ordered AS element_dense_rank_2
    FROM metrics
    WINDOW w_ordered AS (ORDER BY element NULLS LAST)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        effective_from,
        element,
        element_dense_rank,
        element_percent_rank,
        element_dense_rank_2,
        element - element_dense_rank AS element_minus_element_dense_rank
    FROM with_windows
    WHERE element_rank BETWEEN 3 AND 6
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        effective_from,
        element,
        ROUND(element * 100, 2) AS adjusted_element
    FROM filtered
    WHERE effective_from >= DATE '2024-04-05'
      AND element_dense_rank_2 > 5
),
valid_price_tiers AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(adjusted_element, 2) AS adjusted_element_rounded
    FROM valid
    WHERE effective_from > DATE '2024-04-01'
      AND adjusted_element <= 59
)
SELECT effective_from, adjusted_element_rounded
FROM valid_price_tiers
ORDER BY adjusted_element_rounded DESC NULLS LAST
