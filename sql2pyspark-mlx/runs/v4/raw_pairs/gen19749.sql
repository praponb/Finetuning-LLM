SELECT
    oi.id,
    oi.unit_price,
    o.shipping_fee,
    o.discount,
    p.supplier_id,
    p.product_name
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
FULL OUTER JOIN products p ON oi.product_id = p.id;
