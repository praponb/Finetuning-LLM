SELECT *, CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END AS is_singapore, SUBSTRING(supplier_name, 1, 4) AS supplier_name_prefix FROM suppliers WHERE supplier_name IS NOT NULL;
