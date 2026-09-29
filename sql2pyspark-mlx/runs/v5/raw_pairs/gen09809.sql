SELECT
    o.id,
    o.amount,
    c.name,
    c.country,
    s.sqft
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN stores s ON o.store_id = s.id;
