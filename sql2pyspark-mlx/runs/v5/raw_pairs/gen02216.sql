SELECT supplier_name, CONCAT_WS('-', city, country) AS city_country_key FROM suppliers WHERE city IN ('New York', 'Berlin');
