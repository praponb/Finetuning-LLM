SELECT
    o.order_date,
    c.credit_limit,
    c.name,
    s.region,
    s.sqft
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON o.store_id = s.id
WHERE c.email LIKE '%@outlook.com'
ORDER BY order_date
