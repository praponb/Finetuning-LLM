SELECT s.id, s.country, s.city FROM suppliers s WHERE s.rating < 3.9 AND s.id IN (SELECT supplier_id FROM products)
