SELECT id, city, country, rating, supplier_name, FIRST_VALUE(supplier_name) OVER (PARTITION BY city, country ORDER BY rating, id) AS first_supplier_name FROM suppliers WHERE city = 'Tokyo';
