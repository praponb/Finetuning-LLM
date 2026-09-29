SELECT oi.quantity, oi.unit_price, p.category
FROM order_items oi
LEFT OUTER JOIN products p ON p.id = oi.product_id
WHERE oi.order_id IN (7, 16, 25, 37)
ORDER BY 2 DESC;
