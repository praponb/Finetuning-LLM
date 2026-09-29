SELECT
    p.id,
    p.method,
    p.order_id,
    o.store_id,
    s.sqft,
    ADD_MONTHS(o.order_date, 3) order_date_plus_months
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
INNER JOIN stores s ON o.store_id = s.id
WHERE o.shipping_fee < 27
