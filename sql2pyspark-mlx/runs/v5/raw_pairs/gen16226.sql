SELECT
    s.id,
    s.supplier_name,
    s.country,
    s.rating
FROM suppliers s
WHERE s.rating BETWEEN 2.7 AND 3.9 AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id)
ORDER BY supplier_name
