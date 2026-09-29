SELECT suppliers.id, suppliers.city, suppliers.country
FROM suppliers
WHERE EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id AND products.launch_date <= '2025-05-15');
