SELECT oi.unit_price, p.launch_date, p.category
FROM order_items oi
JOIN products p ON oi.product_id = p.id
WHERE oi.unit_price BETWEEN 100 AND 390
