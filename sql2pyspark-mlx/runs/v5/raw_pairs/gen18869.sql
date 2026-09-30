SELECT city
FROM customers
WHERE credit_limit >= 5500
UNION
SELECT city
FROM stores;
