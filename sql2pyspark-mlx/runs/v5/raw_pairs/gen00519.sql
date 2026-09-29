SELECT
    c.id,
    c.name,
    c.credit_limit,
    t.latest_order_date,
    t.completed_count
FROM customers c
JOIN (SELECT customer_id, MAX(order_date) latest_order_date, SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) completed_count
    FROM orders
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.completed_count >= 60;
