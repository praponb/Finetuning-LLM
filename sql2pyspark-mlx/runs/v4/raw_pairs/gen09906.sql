SELECT s.id, s.city, s.country
FROM suppliers s
WHERE s.rating <= 2.0 AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.launch_date >= '2025-06-21')
ORDER BY country DESC
