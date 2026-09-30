SELECT o.status, COUNT(DISTINCT oi.quantity) AS unique_products FROM order_items oi JOIN orders o ON oi.order_id = o.id GROUP BY o.status ORDER BY 2 DESC, status LIMIT 3;
