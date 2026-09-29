WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CEIL(element) AS element_whole,
        CAST(element AS INT) AS element_int
    FROM exploded
    WHERE effective_from > TO_DATE('2024-04-03')
      AND effective_from BETWEEN '2024-03-19' AND '2024-05-10'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        currency,
        element_int,
        element - element_whole AS element_minus_element_whole,
        UPPER(currency) AS currency_upper
    FROM filtered
    WHERE effective_from > DATE '2024-03-25'
      AND YEAR(effective_from) >= 2023
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY element_minus_element_whole NULLS FIRST) AS element_minus_element_whole_rank,
        SUM(element_minus_element_whole) OVER (PARTITION BY currency) AS currency_sum_element_minus_element_whole
    FROM valid
    WHERE currency NOT IN ('THB', 'USD')
),
filtered_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        element_minus_element_whole,
        currency_upper,
        element_minus_element_whole_rank,
        currency_sum_element_minus_element_whole,
        element_int * element_minus_element_whole_rank AS element_int_times_element_minus_element_whole_rank,
        currency_sum_element_minus_element_whole + element_minus_element_whole_rank AS currency_sum_element_minus_element_whole_plus_element_minus_element_whole_rank
    FROM with_windows
    WHERE element_minus_element_whole_rank <= 6
)
SELECT
    element_minus_element_whole,
    element_minus_element_whole_rank,
    currency_sum_element_minus_element_whole,
    element_int_times_element_minus_element_whole_rank,
    currency_sum_element_minus_element_whole_plus_element_minus_element_whole_rank
FROM filtered_price_tiers
ORDER BY currency_sum_element_minus_element_whole_plus_element_minus_element_whole_rank DESC;
