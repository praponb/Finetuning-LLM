SELECT
    o.id,
    o.store_id,
    o.amount,
    c.name,
    c.city,
    s.opened_date
FROM orders o
JOIN customers c ON o.customer_id = c.id
LEFT OUTER JOIN stores s ON o.store_id = s.id
WHERE c.city <> 'Berlin'
