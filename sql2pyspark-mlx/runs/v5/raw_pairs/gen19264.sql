SELECT id, supplier_name, city, LENGTH(supplier_name) AS supplier_name_length FROM suppliers WHERE city IN ('New York', 'Singapore') AND (city = 'Bangkok' OR supplier_name IS NULL);
