SELECT id, city, supplier_name, rating, TRIM(supplier_name) AS clean_supplier_name FROM suppliers WHERE city = 'Chiang Mai'
