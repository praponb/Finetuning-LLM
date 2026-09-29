SELECT
    o.id,
    o.shipping_fee,
    o.order_date,
    s.opened_date,
    s.store_name,
    s.sqft
FROM orders o
LEFT JOIN stores s ON s.id = o.store_id
WHERE MONTH(o.order_date) IN (4, 5, 9)
