WITH lines AS (
    SELECT
        id,
        effective_from,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
)
SELECT
    id,
    effective_from,
    RANK() OVER (ORDER BY element DESC NULLS LAST) AS element_rank,
    DENSE_RANK() OVER (ORDER BY element DESC) AS element_dense_rank
FROM lines
