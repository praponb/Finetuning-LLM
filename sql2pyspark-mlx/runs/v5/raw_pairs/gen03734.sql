SELECT credit_limit, is_active, CONCAT_WS('-', country, name) AS country_name_key
FROM customers
WHERE is_active = TRUE
ORDER BY country_name_key DESC;
