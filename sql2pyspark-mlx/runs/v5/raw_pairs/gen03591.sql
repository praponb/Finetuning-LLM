SELECT
    customers.id,
    customers.country,
    customers.email,
    t.unique_devices,
    t.tablet_count
FROM customers
INNER JOIN (SELECT customer_id, COUNT(DISTINCT device) unique_devices, SUM(CASE WHEN device = 'tablet' THEN 1 ELSE 0 END) tablet_count
    FROM sessions
    WHERE country = 'JP'
    GROUP BY customer_id) t ON customers.id = t.customer_id;
