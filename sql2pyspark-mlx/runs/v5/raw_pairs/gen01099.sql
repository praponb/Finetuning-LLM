SELECT
    s.id,
    s.city,
    s.rating,
    s.country
FROM suppliers s
WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id)
ORDER BY s.id DESC, country;
