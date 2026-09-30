WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(name, ' - ', segment) AS name_segment,
        LOWER(email) AS email_lower,
        COALESCE(segment, 'DIRECT') AS segment_clean
    FROM customers
    WHERE segment IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    name_segment,
    email_lower,
    segment_clean,
    MAX(age) OVER (PARTITION BY city) AS city_max_age,
    LEAD(age, 2) OVER (ORDER BY signup_date, id) AS next_age
FROM filtered
