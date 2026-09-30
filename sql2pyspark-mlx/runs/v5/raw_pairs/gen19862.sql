SELECT id, name, CONCAT_WS(',', name, city) AS name_city_key FROM customers WHERE credit_limit BETWEEN 5900 AND 14100 ORDER BY name_city_key;
