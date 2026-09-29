WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) t AS element
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(element AS INT) AS element_int,
        CAST(element AS INT) AS element_int_2
    FROM lines
    WHERE element >= 45
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        element_int,
        element_int_2,
        CAST(element_int_2 AS STRING) AS element_int_2_num
    FROM valid
    WHERE element_int BETWEEN 53 AND 58
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        element_int,
        element_int_2_num,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element_int_2 NULLS LAST) AS element_int_2_cume_dist,
        AVG(element_int_2) OVER (PARTITION BY plan) AS plan_avg_element_int_2
    FROM base
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LEAST(element_int, plan_avg_element_int_2) AS least_element_int_plan_avg_element_int_2
    FROM scored
    WHERE plan_avg_element_int_2 BETWEEN 59 AND 78
)
SELECT
    id,
    element_int_2_num,
    element_int_2_cume_dist,
    plan_avg_element_int_2,
    least_element_int_plan_avg_element_int_2
FROM cleaned
ORDER BY id DESC;
