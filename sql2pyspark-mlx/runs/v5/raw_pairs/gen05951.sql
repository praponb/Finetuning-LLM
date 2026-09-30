SELECT
    o.id,
    o.store_id,
    o.customer_id,
    c.credit_limit,
    c.name
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
WHERE c.age BETWEEN 41 AND 46
ORDER BY o.customer_id;
