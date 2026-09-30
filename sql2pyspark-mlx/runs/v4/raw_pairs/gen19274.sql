SELECT supplier_name, rating, country, COALESCE(supplier_name, 'unassigned') AS supplier_name_filled, REGEXP_REPLACE(supplier_name, ' ', '') AS supplier_name_slug FROM suppliers
