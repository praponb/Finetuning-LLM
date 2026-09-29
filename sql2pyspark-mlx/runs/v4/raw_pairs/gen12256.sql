WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS element
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(element) OVER (PARTITION BY currency) AS currency_avg_element,
        RANK() OVER (PARTITION BY currency ORDER BY element NULLS LAST) AS element_rank,
        COUNT(element) OVER (PARTITION BY currency) AS currency_count_element,
        RANK() OVER (ORDER BY element) AS element_rank_2
    FROM flattened
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        effective_from,
        currency,
        element,
        currency_avg_element,
        element_rank,
        currency_count_element,
        element_rank_2,
        INITCAP(TRIM(currency)) AS currency_title
    FROM scored
    WHERE effective_from > DATE '2024-06-10'
      AND element_rank >= 6
)
SELECT
    id,
    currency,
    currency_avg_element,
    element_rank,
    currency_title
FROM cleaned
ORDER BY id DESC;
