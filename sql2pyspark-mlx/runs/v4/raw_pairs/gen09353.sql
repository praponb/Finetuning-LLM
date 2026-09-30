SELECT oi.unit_price, p.category, o.store_id
FROM order_items oi
JOIN products p ON oi.product_id = p.id
FULL OUTER JOIN orders o ON oi.order_id = o.id
