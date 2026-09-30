SELECT s.id, s.city, s.supplier_name
FROM suppliers s
WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.launch_date >= '2023-05-27')
