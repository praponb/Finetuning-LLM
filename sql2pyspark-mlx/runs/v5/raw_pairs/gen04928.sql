WITH customer_summary AS (
    SELECT customer_id, COUNT(DISTINCT balance) AS unique_customers, MIN(opened_date) AS first_opened_date
    FROM accounts
    GROUP BY customer_id
)
SELECT
    c.id,
    c.credit_limit,
    c.email,
    cs.unique_customers,
    cs.first_opened_date
FROM customers c
INNER JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.unique_customers > 8
