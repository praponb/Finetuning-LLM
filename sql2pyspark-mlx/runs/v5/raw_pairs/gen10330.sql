SELECT country, city, MAX(credit_limit) AS max_credit_limit
FROM customers
WHERE NOT is_active AND (age >= 54 OR segment = 'SMB')
GROUP BY country, city
ORDER BY country
