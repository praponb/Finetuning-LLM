SELECT
    o.id,
    o.amount,
    o.customer_id,
    o.store_id,
    s.region
FROM orders o
INNER JOIN stores s ON s.id = o.store_id
WHERE s.city NOT IN ('Tokyo', 'New York', 'Singapore');
