SELECT
    p.order_id,
    o.amount,
    s.store_name,
    s.sqft
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
LEFT OUTER JOIN stores s ON o.store_id = s.id;
