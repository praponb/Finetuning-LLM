SELECT
    oi.id,
    oi.unit_price,
    oi.product_id,
    oi.quantity,
    p.product_name,
    p.category,
    p.cost
FROM order_items oi
LEFT JOIN products p ON p.id = oi.product_id
WHERE oi.unit_price < 160;
