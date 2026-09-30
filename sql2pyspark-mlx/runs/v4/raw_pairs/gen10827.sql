WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        UPPER(city) AS city_upper,
        COALESCE(segment, 'UNKNOWN') AS resolved_segment
    FROM customers
    WHERE signup_date >= TO_DATE('2024-03-03')
)
-- Ranking and gap features
SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    city_upper,
    resolved_segment,
    LAST_VALUE(segment, TRUE) OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_segment,
    ROW_NUMBER() OVER (ORDER BY signup_date, id) AS seq_no,
    FIRST_VALUE(segment) IGNORE NULLS OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_segment
FROM filtered
