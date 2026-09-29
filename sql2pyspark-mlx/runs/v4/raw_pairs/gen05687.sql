SELECT
    s.id,
    s.country,
    COUNT(p.id) AS product_count,
    SUM(CASE WHEN p.brand = 'Acme' THEN p.price ELSE 0 END) AS acme_price
FROM suppliers s
JOIN products p ON s.id = p.supplier_id
GROUP BY s.id, s.country
