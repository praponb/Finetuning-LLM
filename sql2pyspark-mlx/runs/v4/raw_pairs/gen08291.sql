SELECT
    o.amount,
    o.discount,
    s.region,
    c.segment,
    c.city
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
LEFT JOIN customers c ON o.customer_id = c.id
