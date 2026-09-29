SELECT oi.order_id, p.launch_date
FROM order_items oi
INNER JOIN products p ON p.id = oi.product_id
WHERE oi.order_id IN (2, 14, 21, 32)
ORDER BY launch_date;
