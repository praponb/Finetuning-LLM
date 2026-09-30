SELECT
    s.id,
    s.shipped_date,
    s.status,
    o.amount,
    o.shipping_fee,
    o.discount
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
ORDER BY amount DESC;
