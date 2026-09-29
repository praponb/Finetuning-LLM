SELECT
    s.id,
    COUNT(p.id) AS product_count,
    ROUND(STDDEV(p.cost), 2) AS stddev_cost,
    SUM(CASE WHEN p.brand = 'Initech' THEN p.price ELSE 0 END) AS initech_price
FROM suppliers s
JOIN products p ON p.supplier_id = s.id
WHERE s.city = 'Singapore'
GROUP BY s.id
HAVING COUNT(p.id) >= 1;
