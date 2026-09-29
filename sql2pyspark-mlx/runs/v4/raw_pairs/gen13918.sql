SELECT
    p.category,
    s.rating,
    s.supplier_name,
    s.country
FROM products p
RIGHT JOIN suppliers s ON s.id = p.supplier_id
WHERE ARRAY_CONTAINS(p.tags, 'premium') AND s.city = 'Tokyo';
