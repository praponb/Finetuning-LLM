SELECT city, 'supplier' AS kind
FROM suppliers
WHERE city = 'Singapore'
UNION ALL
SELECT city, 'store' AS kind
FROM stores
WHERE LENGTH(store_name) < 6
