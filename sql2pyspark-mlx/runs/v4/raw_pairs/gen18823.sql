SELECT id, city, supplier_name, country, TRIM(supplier_name) clean_supplier_name FROM suppliers WHERE LENGTH(supplier_name) < 9;
