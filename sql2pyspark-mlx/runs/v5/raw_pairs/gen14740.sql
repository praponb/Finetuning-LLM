SELECT id, rating, LOWER(supplier_name) supplier_name_lower FROM suppliers WHERE LOWER(TRIM(supplier_name)) = 'global supply';
