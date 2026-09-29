SELECT
    o.id,
    o.shipping_fee,
    o.status,
    c.is_active,
    c.email,
    s.city
FROM orders o
JOIN customers c ON c.id = o.customer_id
JOIN stores s ON o.store_id = s.id
ORDER BY status DESC;
