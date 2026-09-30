SELECT id, country, UPPER(country) AS country_upper, CONCAT_WS(',', supplier_name, country) AS supplier_name_country_key FROM suppliers;
