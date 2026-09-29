SELECT
    o.id,
    o.store_id,
    c.country,
    s.region,
    s.opened_date
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON s.id = o.store_id
