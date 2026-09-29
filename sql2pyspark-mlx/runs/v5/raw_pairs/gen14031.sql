SELECT
    o.id,
    o.order_date,
    c.email,
    s.region
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON o.store_id = s.id
ORDER BY 4 DESC;
