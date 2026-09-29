SELECT store_id, sale_date
FROM store_sales
WHERE units >= 37
INTERSECT
SELECT store_id, sale_date
FROM store_sales
WHERE store_id IN (1, 2, 3)
