SELECT DATE_FORMAT(o.order_date, 'yyyy-MM') AS order_month, COUNT(DISTINCT oi.quantity) AS unique_products, MAX(oi.quantity) - MIN(oi.quantity) AS quantity_range
FROM order_items oi
JOIN products p ON oi.product_id = p.id
JOIN orders o ON oi.order_id = o.id
WHERE o.shipping_fee < 26
GROUP BY DATE_FORMAT(o.order_date, 'yyyy-MM')
ORDER BY 2 DESC;
