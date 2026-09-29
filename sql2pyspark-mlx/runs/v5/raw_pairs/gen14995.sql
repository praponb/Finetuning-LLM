SELECT
    o.id,
    o.shipping_fee,
    o.status,
    o.order_date,
    s.store_name
FROM orders o
RIGHT OUTER JOIN stores s ON s.id = o.store_id
ORDER BY status
