WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        COALESCE(name, 'N/A') name_filled,
        COALESCE(city, 'NONE') city_clean
    FROM customers
    WHERE signup_date >= DATE '2023-10-16'
      AND segment IS NOT NULL
),
cleaned_customers AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(segment) segment_length
    FROM cleaned
    WHERE age <= 39
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(segment_length / credit_limit, 2) segment_length_per_credit_limit
    FROM cleaned_customers
    WHERE credit_limit BETWEEN 5100 AND 7900
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        age + credit_limit age_plus_credit_limit,
        ROUND(segment_length / age, 2) segment_length_per_age
    FROM base
    WHERE segment_length_per_credit_limit IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        segment,
        segment_length_per_credit_limit,
        age_plus_credit_limit,
        credit_limit
    FROM valid
) src
UNPIVOT INCLUDE NULLS (
    val FOR metric_name IN (segment_length_per_credit_limit, age_plus_credit_limit, credit_limit)
)
