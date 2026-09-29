SELECT
    oi.order_id,
    o.store_id,
    s.sqft,
    s.opened_date
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
INNER JOIN stores s ON s.id = o.store_id
