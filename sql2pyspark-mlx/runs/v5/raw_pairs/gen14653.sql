WITH customer_stats AS (
    SELECT customer_id, MIN(balance) AS min_balance, COUNT(DISTINCT balance) AS unique_customers
    FROM accounts
    GROUP BY customer_id
)
SELECT
    c.id,
    c.email,
    c.country,
    cs.min_balance,
    cs.unique_customers
FROM customers c
LEFT JOIN customer_stats cs ON c.id = cs.customer_id
