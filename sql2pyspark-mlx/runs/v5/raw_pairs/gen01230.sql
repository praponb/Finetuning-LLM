WITH top_customers AS (
    SELECT
        id,
        country,
        age,
        signup_date,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY age DESC) AS rnk
    FROM customers
)
SELECT *
FROM top_customers
WHERE rnk <= 5;
