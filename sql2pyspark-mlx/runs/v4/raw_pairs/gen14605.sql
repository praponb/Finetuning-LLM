SELECT p.id, p.supplier_id FROM products p WHERE p.launch_date > '2024-11-01' AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.revenue >= 2700) ORDER BY supplier_id;
