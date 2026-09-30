WITH orders_by_customer AS (
    SELECT customer_id, COUNT(*) AS num_orders
    FROM orders
    WHERE status IN ('RETURNED', 'SHIPPED', 'CANCELLED')
    GROUP BY customer_id
)
SELECT c.name, obc.num_orders
FROM customers c
INNER JOIN orders_by_customer obc ON c.id = obc.customer_id
ORDER BY num_orders DESC;
