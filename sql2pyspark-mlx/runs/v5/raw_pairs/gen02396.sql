WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(segment) AS segment_lower,
        LOWER(segment) AS segment_lower_2,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM customers
    WHERE signup_date >= DATE '2023-09-15'
)
-- Window metrics over several different partitions
SELECT
    *,
    CORR(age, credit_limit) OVER (PARTITION BY country, city) AS corr_age_credit_limit,
    LEAD(credit_limit, 2) OVER (PARTITION BY city ORDER BY signup_date, id) AS next_credit_limit,
    FIRST_VALUE(age, TRUE) OVER (ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age
FROM cleaned
WHERE is_active
