SELECT
    oi.id,
    oi.unit_price,
    p.cost,
    o.customer_id,
    o.discount,
    ROUND(p.cost, 1) AS cost_rounded
FROM order_items oi
RIGHT JOIN products p ON oi.product_id = p.id
LEFT JOIN orders o ON oi.order_id = o.id
ORDER BY discount DESC
