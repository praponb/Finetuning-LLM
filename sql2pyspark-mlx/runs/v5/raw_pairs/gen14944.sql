SELECT
    o.id,
    o.channel,
    c.email,
    s.store_name
FROM orders o
RIGHT OUTER JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON s.id = o.store_id;
