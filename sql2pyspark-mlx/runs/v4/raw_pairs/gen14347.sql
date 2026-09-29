SELECT
    o.id,
    o.customer_id,
    o.store_id,
    s.store_name,
    c.age
FROM orders o
RIGHT OUTER JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON o.customer_id = c.id
