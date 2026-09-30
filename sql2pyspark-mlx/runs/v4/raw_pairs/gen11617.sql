SELECT
    customers.id,
    customers.signup_date,
    t.pending_count,
    t.stddev_discount
FROM customers
LEFT JOIN (SELECT customer_id, SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count, ROUND(STDDEV(discount), 2) AS stddev_discount
    FROM orders
    GROUP BY customer_id) t ON customers.id = t.customer_id
WHERE t.pending_count > 57;
