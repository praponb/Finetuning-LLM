SELECT
    o.id,
    o.discount,
    o.customer_id,
    s.sqft,
    c.is_active
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON o.customer_id = c.id
ORDER BY o.customer_id
