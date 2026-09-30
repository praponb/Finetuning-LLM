SELECT city
FROM customers
WHERE city = 'Tokyo'
UNION
SELECT city
FROM stores;
