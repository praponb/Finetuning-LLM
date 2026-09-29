WITH ranked_customers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit DESC, id) AS rnk
    FROM customers
)
SELECT id, city, credit_limit
FROM ranked_customers
WHERE rnk <= 5
ORDER BY credit_limit DESC
