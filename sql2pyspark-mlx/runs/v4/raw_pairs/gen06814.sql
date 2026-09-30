SELECT
    p.id,
    p.status,
    o.customer_id,
    o.store_id,
    s.store_name,
    s.sqft
FROM payments p
JOIN orders o ON p.order_id = o.id
JOIN stores s ON o.store_id = s.id
