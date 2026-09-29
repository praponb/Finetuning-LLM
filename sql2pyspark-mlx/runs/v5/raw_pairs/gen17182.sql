SELECT s.id, s.supplier_name, s.city, s.country FROM suppliers s WHERE s.supplier_name IS NOT NULL AND s.id IN (SELECT supplier_id FROM products WHERE brand NOT IN ('Acme', 'Globex', 'Umbrella'));
