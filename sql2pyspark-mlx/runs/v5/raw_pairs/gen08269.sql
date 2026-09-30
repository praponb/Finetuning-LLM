WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age ASC NULLS LAST, id ASC) AS latest_rank
    FROM customers
    QUALIFY latest_rank = 1
)
SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    is_active,
    age
FROM latest
ORDER BY country DESC
