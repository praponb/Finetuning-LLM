SELECT DISTINCT region
FROM stores
WHERE LENGTH(store_name) < 13
ORDER BY region ASC;
