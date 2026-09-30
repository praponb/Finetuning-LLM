SELECT DISTINCT region
FROM stores
WHERE store_name IS NOT NULL
ORDER BY region DESC;
