WITH top_customers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS row_num
    FROM customers
    WHERE segment = 'Consumer'
)
SELECT
    id,
    country,
    signup_date,
    name
FROM top_customers
WHERE row_num <= 1
ORDER BY signup_date, name DESC;
