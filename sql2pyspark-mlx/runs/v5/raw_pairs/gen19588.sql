SELECT o.status, ROUND(STDDEV(oi.quantity), 2) AS stddev_quantity
FROM order_items oi
LEFT JOIN orders o ON o.id = oi.order_id
GROUP BY o.status
ORDER BY status, stddev_quantity DESC;
