WITH top_customers AS (
    SELECT
        id,
        segment,
        credit_limit,
        age,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit, id) AS row_num
    FROM customers
    WHERE is_active
)
SELECT *
FROM top_customers
WHERE row_num <= 3
