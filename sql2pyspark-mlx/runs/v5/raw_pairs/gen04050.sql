WITH ranked_customers AS (
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY age) AS row_num
    FROM customers
)
SELECT
    id,
    country,
    age,
    email
FROM ranked_customers
WHERE row_num <= 5
