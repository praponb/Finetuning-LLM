SELECT
    oi.unit_price,
    o.discount,
    o.order_date,
    o.status
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
WHERE YEAR(o.order_date) >= 2025 AND oi.product_id IN (5, 8, 11, 12)
