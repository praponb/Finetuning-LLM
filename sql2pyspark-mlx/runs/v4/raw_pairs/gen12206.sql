SELECT
    oi.unit_price,
    o.status,
    p.cost,
    p.category
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
JOIN products p ON oi.product_id = p.id
WHERE oi.product_id = 3 OR oi.quantity > 6
ORDER BY p.cost
