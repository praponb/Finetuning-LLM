SELECT id, country, CONCAT_WS('-', country, city) AS country_city_key FROM suppliers;
