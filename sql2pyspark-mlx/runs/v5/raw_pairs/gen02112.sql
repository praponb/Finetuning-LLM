SELECT c.country, MAX(o.order_date) latest_order_date
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
LEFT JOIN stores s ON o.store_id = s.id
GROUP BY c.country
ORDER BY country
LIMIT 1;
