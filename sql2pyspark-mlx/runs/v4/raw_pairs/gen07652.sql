SELECT id, is_active, name, age, CONCAT_WS('-', country, name) AS country_name_key FROM customers ORDER BY age, id LIMIT 3;
