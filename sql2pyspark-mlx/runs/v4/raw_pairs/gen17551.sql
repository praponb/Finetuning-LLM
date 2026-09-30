SELECT s.id, s.country, s.city FROM suppliers s WHERE s.country = 'DE' AND s.id IN (SELECT supplier_id FROM products WHERE cost >= 190);
