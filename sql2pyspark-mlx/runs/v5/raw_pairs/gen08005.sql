SELECT
    o.id,
    o.store_id,
    o.customer_id,
    c.name
FROM orders o
FULL OUTER JOIN customers c ON o.customer_id = c.id
WHERE o.amount BETWEEN 960 AND 1110
