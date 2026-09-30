SELECT
    oi.id,
    oi.unit_price,
    o.store_id,
    o.order_date,
    p.brand,
    p.stock_qty
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
LEFT OUTER JOIN products p ON oi.product_id = p.id
WHERE p.supplier_id = 28 AND ARRAY_CONTAINS(p.tags, 'eco')
