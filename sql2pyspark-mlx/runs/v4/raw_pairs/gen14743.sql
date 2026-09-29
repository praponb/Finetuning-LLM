WITH customer_summary AS (
    SELECT customer_id, COUNT(CASE WHEN plan = 'standard' THEN 1 END) AS standard_count
    FROM subscriptions
    GROUP BY customer_id
)
SELECT c.is_active, c.credit_limit, cs.standard_count
FROM customers c
LEFT JOIN customer_summary cs ON c.id = cs.customer_id
ORDER BY cs.standard_count DESC, c.credit_limit DESC
