SELECT product_id, store_id
FROM store_sales
WHERE revenue >= 1000
UNION
SELECT product_id, store_id
FROM store_sales
WHERE product_id = 1
