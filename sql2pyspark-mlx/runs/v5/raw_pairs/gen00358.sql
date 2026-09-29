WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(discounts) AS element
    FROM price_tiers
),
filtered AS (
    SELECT
        *,
        UPPER(plan) AS plan_upper,
        UPPER(plan) AS plan_upper_2
    FROM lines
    WHERE effective_from > TO_DATE('2024-05-06')
),
scored AS (
    SELECT
        id,
        effective_from,
        plan_upper,
        RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS LAST) AS element_rank
    FROM filtered
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY element_rank DESC NULLS LAST) AS element_rank_dense_rank,
        RANK() OVER (ORDER BY element_rank DESC) AS element_rank_rank,
        PERCENT_RANK() OVER (ORDER BY element_rank DESC NULLS FIRST) AS element_rank_percent_rank
    FROM scored
),
filtered_price_tiers AS (
    SELECT *, ROUND(element_rank_percent_rank / element_rank_rank, 2) AS element_rank_percent_rank_per_element_rank_rank
    FROM with_windows
    WHERE effective_from > DATE '2024-05-09'
)
SELECT
    id,
    effective_from,
    plan_upper,
    element_rank,
    element_rank_percent_rank,
    element_rank_percent_rank_per_element_rank_rank
FROM filtered_price_tiers
ORDER BY effective_from DESC NULLS FIRST;
