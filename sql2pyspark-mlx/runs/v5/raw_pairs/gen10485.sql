SELECT 'supplier' AS kind, city
FROM suppliers
UNION ALL
SELECT 'store' AS kind, city
FROM stores
ORDER BY kind
