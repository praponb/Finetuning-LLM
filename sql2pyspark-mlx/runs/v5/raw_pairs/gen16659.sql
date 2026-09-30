SELECT
    p.id,
    p.order_id,
    o.shipping_fee,
    c.segment,
    CAST(c.is_active AS INT) AS is_active_flag
FROM payments p
RIGHT JOIN orders o ON p.order_id = o.id
RIGHT JOIN customers c ON o.customer_id = c.id
WHERE NOT (LOWER(c.name) LIKE '%ob %' OR c.country = 'TH');
