SELECT s.id, s.supplier_name, s.rating
FROM suppliers s
WHERE s.id IN (SELECT supplier_id FROM products WHERE SIZE(tags) >= 2)
