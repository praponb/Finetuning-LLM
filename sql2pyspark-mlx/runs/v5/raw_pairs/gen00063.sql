SELECT email, CONCAT_WS(',', country, email) AS country_email_key FROM customers WHERE city = 'Tokyo' ORDER BY country_email_key ASC;
