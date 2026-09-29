SELECT id, supplier_name, TRIM(city) AS clean_city, LOWER(supplier_name) AS supplier_name_lower FROM suppliers WHERE country IN ('JP', 'UK', 'DE') AND supplier_name LIKE 'E%';
