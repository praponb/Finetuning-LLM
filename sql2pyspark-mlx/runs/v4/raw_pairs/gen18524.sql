SELECT
    oi.unit_price,
    o.discount,
    o.store_id,
    p.price,
    ROUND(p.cost * 1.15, 2) AS adjusted_cost
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
ORDER BY o.discount DESC
