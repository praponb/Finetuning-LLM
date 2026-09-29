SELECT s.sqft, ss.units
FROM stores s
INNER JOIN store_sales ss ON s.id = ss.store_id
WHERE UPPER(s.store_name) LIKE '%OWN%' OR ss.revenue >= 3700;
