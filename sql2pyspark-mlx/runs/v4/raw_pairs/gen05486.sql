SELECT id, city, country FROM suppliers WHERE rating IS NULL OR LENGTH(supplier_name) < 8
