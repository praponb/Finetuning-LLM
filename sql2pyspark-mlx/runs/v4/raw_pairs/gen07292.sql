SELECT id, units, store_id
FROM store_sales
WHERE revenue > 3600
UNION
SELECT id, units, store_id
FROM store_sales
WHERE store_id IN (1, 2, 4);
