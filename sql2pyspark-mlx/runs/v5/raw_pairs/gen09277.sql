SELECT city
FROM customers
WHERE is_active
EXCEPT
SELECT city
FROM stores;
