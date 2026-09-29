SELECT id, city, supplier_name, rating, SUBSTRING(country, 1, 3) country_prefix, CONCAT_WS('/', city, country) city_country_key FROM suppliers ORDER BY city, id LIMIT 5
