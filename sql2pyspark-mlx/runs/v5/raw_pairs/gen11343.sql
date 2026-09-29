SELECT rating, country, city, CONCAT_WS('-', country, supplier_name) AS country_supplier_name_key FROM suppliers WHERE UPPER(supplier_name) LIKE '%IAM%'
