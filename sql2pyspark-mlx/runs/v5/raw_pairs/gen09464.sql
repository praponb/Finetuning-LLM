SELECT
    o.id,
    o.status,
    o.discount,
    c.is_active,
    c.email,
    s.city,
    s.opened_date
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
LEFT JOIN stores s ON s.id = o.store_id
WHERE c.country = 'UK';
