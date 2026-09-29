SELECT suppliers.id, suppliers.rating, suppliers.country, suppliers.city FROM suppliers WHERE suppliers.id IN (SELECT supplier_id FROM products) ORDER BY country ASC, city ASC;
