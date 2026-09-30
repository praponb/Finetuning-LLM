SELECT
    o.id,
    o.status,
    o.amount,
    c.email,
    s.opened_date,
    s.sqft
FROM orders o
LEFT OUTER JOIN customers c ON o.customer_id = c.id
JOIN stores s ON s.id = o.store_id
WHERE s.city <> 'Berlin'
ORDER BY status, id
LIMIT 10;
