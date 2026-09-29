WITH top_customers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) rn
    FROM customers
)
SELECT
    id,
    country,
    signup_date,
    credit_limit
FROM top_customers
WHERE rn <= 3
ORDER BY signup_date;
