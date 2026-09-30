SELECT country, is_active, age
FROM customers
WHERE credit_limit > 11800 AND country != 'US'
ORDER BY country;
