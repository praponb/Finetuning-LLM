SELECT id, supplier_name, COALESCE(rating, 0) AS rating_filled, CONCAT_WS('-', supplier_name, country) AS supplier_name_country_key FROM suppliers WHERE rating > 3.8
