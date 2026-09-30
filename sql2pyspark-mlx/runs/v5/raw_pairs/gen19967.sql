WITH customer_summary AS (
    SELECT customer_id, AVG(discount) avg_discount
    FROM orders
    GROUP BY customer_id
)
SELECT c.id, c.city, cs.avg_discount
FROM customers c
LEFT OUTER JOIN customer_summary cs ON c.id = cs.customer_id;
