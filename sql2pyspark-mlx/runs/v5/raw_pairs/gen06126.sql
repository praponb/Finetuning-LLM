SELECT is_active, credit_limit, CONCAT_WS('/', city, name) AS city_name_key
FROM customers;
