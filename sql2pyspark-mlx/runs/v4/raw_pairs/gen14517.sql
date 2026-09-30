WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(discounts) AS element
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    *,
    RANK() OVER (PARTITION BY currency ORDER BY element DESC NULLS LAST) AS element_rank,
    PERCENT_RANK() OVER (ORDER BY element ASC) AS element_percent_rank,
    DENSE_RANK() OVER (ORDER BY element DESC NULLS FIRST) AS element_dense_rank
FROM exploded
