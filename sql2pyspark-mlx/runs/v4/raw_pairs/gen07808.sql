SELECT
    o.status,
    s.region,
    s.store_name,
    c.email,
    c.country
FROM orders o
JOIN stores s ON o.store_id = s.id
LEFT JOIN customers c ON o.customer_id = c.id
WHERE o.store_id = 18;
