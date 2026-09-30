WITH orders_by_customer AS (
    SELECT customer_id, COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS completed_count, COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count
    FROM orders
    GROUP BY customer_id
)
SELECT c.age, obc.completed_count, obc.mobile_count
FROM customers c
JOIN orders_by_customer obc ON c.id = obc.customer_id;
