WITH ranked_customers AS (
    SELECT
        id,
        city,
        credit_limit,
        age,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS rnk
    FROM customers
    WHERE city IN ('Berlin', 'Bangkok')
)
SELECT *
FROM ranked_customers
WHERE rnk <= 3
ORDER BY age DESC, city DESC;
