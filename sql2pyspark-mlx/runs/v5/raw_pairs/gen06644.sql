SELECT id, city, supplier_name, COALESCE(supplier_name, 'N/A') AS supplier_name_filled FROM suppliers WHERE country = 'TH'
