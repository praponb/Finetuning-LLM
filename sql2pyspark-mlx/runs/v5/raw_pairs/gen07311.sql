SELECT city
FROM customers
WHERE is_active
UNION ALL
SELECT city
FROM stores
