SELECT
    o.id,
    o.amount,
    s.city,
    s.opened_date,
    c.email,
    c.is_active,
    LOWER(s.region) AS region_lower
FROM orders o
LEFT JOIN stores s ON s.id = o.store_id
INNER JOIN customers c ON o.customer_id = c.id
