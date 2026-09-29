SELECT
    o.shipping_fee,
    c.name,
    s.city,
    s.sqft
FROM orders o
LEFT OUTER JOIN customers c ON c.id = o.customer_id
INNER JOIN stores s ON o.store_id = s.id
