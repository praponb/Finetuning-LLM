WITH customer_stats AS (
    SELECT customer_id, COUNT(DISTINCT amount) unique_statuss
    FROM orders
    WHERE status = 'SHIPPED'
    GROUP BY customer_id
)
SELECT c.email, c.credit_limit, cs.unique_statuss
FROM customers c
INNER JOIN customer_stats cs ON c.id = cs.customer_id
WHERE cs.unique_statuss > 8;
