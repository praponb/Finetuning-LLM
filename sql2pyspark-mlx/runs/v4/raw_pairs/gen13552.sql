SELECT *, TRIM(supplier_name) AS clean_supplier_name, FLOOR(rating) AS rating_whole FROM suppliers WHERE supplier_name IS NOT NULL ORDER BY country;
