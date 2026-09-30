SELECT
    p.order_id,
    p.status,
    o.order_date,
    o.amount,
    s.sqft,
    s.store_name
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
FULL OUTER JOIN stores s ON o.store_id = s.id
