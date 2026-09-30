SELECT
    o.id,
    o.customer_id,
    o.status,
    s.opened_date
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
WHERE UPPER(TRIM(s.store_name)) = 'DOWNTOWN EXPRESS';
