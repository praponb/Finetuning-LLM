SELECT city, country FROM suppliers WHERE country = 'JP' UNION SELECT city, country FROM suppliers WHERE LOWER(TRIM(supplier_name)) = 'pacific wholesale' ORDER BY country
