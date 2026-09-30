SELECT id, country, CONCAT_WS('-', supplier_name, city) AS supplier_name_city_key FROM suppliers WHERE country IN ('SG', 'UK') AND (UPPER(supplier_name) LIKE '%ACI%' OR city != 'London');
