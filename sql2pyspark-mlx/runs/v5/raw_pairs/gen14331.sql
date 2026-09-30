WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        MAX(element) OVER (PARTITION BY currency) AS currency_max_element,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element) AS element_dense_rank
    FROM exploded
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        currency_max_element,
        element_dense_rank,
        PERCENT_RANK() OVER (ORDER BY element_dense_rank) AS element_dense_rank_percent_rank,
        DENSE_RANK() OVER (ORDER BY element_dense_rank DESC NULLS LAST) AS element_dense_rank_dense_rank
    FROM scored
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        currency_max_element + element_dense_rank_dense_rank AS currency_max_element_plus_element_dense_rank_dense_rank,
        CEIL(element_dense_rank) AS element_dense_rank_whole
    FROM with_windows
    WHERE element_dense_rank_dense_rank BETWEEN 3 AND 8
)
SELECT
    id,
    currency_max_element,
    element_dense_rank,
    currency_max_element_plus_element_dense_rank_dense_rank
FROM valid
ORDER BY currency_max_element_plus_element_dense_rank_dense_rank DESC NULLS FIRST
