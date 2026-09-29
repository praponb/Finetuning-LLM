WITH deduped AS (
    SELECT *
    FROM customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit DESC, id) = 1
)
SELECT
    id,
    email,
    country,
    city,
    segment,
    signup_date,
    credit_limit,
    CASE WHEN country RLIKE '^(DE|UK)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS country_zone
FROM deduped
ORDER BY credit_limit
