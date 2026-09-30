SELECT id, age, CONCAT_WS(',', country, name) AS country_name_key
FROM customers;
