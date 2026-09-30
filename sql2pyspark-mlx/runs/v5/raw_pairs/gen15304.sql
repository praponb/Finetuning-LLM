SELECT p.category, DATE_TRUNC('MONTH', p.launch_date) AS launch_month, SUM(oi.unit_price) AS total_unit_price
FROM order_items oi
JOIN products p ON oi.product_id = p.id
GROUP BY p.category, DATE_TRUNC('MONTH', p.launch_date);
