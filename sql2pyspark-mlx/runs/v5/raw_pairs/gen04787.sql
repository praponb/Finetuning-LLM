WITH top_customers AS (
    SELECT
        id,
        country,
        credit_limit,
        is_active,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS rnk
    FROM customers
)
SELECT
    id,
    country,
    credit_limit,
    is_active
FROM top_customers
WHERE rnk = 1
ORDER BY id DESC
