SELECT id, city, supplier_name, rating, CONCAT(country, ' ', supplier_name) AS country_supplier_name FROM suppliers WHERE city IS NULL OR rating > 2.9 ORDER BY rating DESC, country_supplier_name;
