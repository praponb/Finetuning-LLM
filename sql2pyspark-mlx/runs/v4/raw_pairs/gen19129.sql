SELECT id, supplier_name, rating, city, TRIM(supplier_name) AS clean_supplier_name FROM suppliers WHERE rating >= 2.6 AND rating > 3.9
