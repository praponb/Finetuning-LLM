SELECT
    s.*,
    o.channel,
    o.discount,
    o.order_date
FROM shipments s
LEFT JOIN orders o
    ON s.order_id = o.id
WHERE o.shipping_fee IS NOT NULL
