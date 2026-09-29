SELECT
    p.brand,
    s.country,
    s.supplier_name,
    s.rating
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.supplier_id IN (13, 38)
ORDER BY s.country DESC;
