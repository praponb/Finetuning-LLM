SELECT country, 'customer' AS kind FROM customers UNION ALL SELECT country, 'session' AS kind FROM sessions WHERE browser <> 'Firefox' ORDER BY country;
