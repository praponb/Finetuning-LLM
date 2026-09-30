SELECT
    o.id,
    o.store_id,
    o.channel,
    s.city,
    s.opened_date,
    c.credit_limit
FROM orders o
INNER JOIN stores s ON s.id = o.store_id
RIGHT JOIN customers c ON c.id = o.customer_id
WHERE c.segment IS NULL AND c.segment = 'SMB';
