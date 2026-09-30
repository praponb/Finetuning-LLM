SELECT oi.id, oi.order_id, p.price
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
WHERE oi.unit_price BETWEEN 140 AND 260
ORDER BY p.price;
