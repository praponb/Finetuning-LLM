SELECT
    p.city,
    e.status,
    COUNT(*) AS cnt,
    MAX(e.discount) AS max_discount
FROM orders e
JOIN customers p ON e.customer_id = p.id
GROUP BY GROUPING SETS ((p.city, e.status), (p.city), ())
