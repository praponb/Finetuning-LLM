SELECT s.id, s.supplier_name FROM suppliers s WHERE s.country <> 'JP' AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND YEAR(p.launch_date) = 2023) ORDER BY s.id DESC;
