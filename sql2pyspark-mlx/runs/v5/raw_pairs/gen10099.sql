SELECT s.city, MAX(o.order_date) AS latest_order_date
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON c.id = o.customer_id
GROUP BY s.city
ORDER BY latest_order_date DESC;
