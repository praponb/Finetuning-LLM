SELECT launch_date AS event_date
FROM products
INTERSECT
SELECT signup_date AS event_date
FROM customers
WHERE segment = 'SMB'
ORDER BY event_date DESC;
