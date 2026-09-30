SELECT
    oi.unit_price,
    p.supplier_id,
    p.product_name,
    o.shipping_fee,
    SUBSTRING(o.status, 1, 4) AS status_prefix
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
WHERE o.channel IN ('web', 'mobile', 'partner') AND p.brand IN ('Initech', 'Umbrella');
