SELECT id, city, country FROM suppliers WHERE supplier_name IS NOT NULL UNION SELECT id, city, country FROM suppliers WHERE supplier_name NOT LIKE 'P%' ORDER BY country;
