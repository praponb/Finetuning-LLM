SELECT MONTH(o.order_date) AS order_month, ROUND(AVG(oi.quantity), 2) AS avg_quantity
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
JOIN orders o ON oi.order_id = o.id
WHERE p.price <= 350
GROUP BY MONTH(o.order_date)
ORDER BY avg_quantity
