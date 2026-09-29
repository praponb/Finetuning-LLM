WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, segment ORDER BY age DESC NULLS LAST, id ASC) AS row_num
        FROM customers
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    name,
    email,
    country,
    city,
    signup_date,
    is_active,
    credit_limit,
    age
FROM latest
ORDER BY signup_date DESC NULLS LAST;
