SELECT product_id, store_id
FROM store_sales
EXCEPT
SELECT product_id, store_id
FROM store_sales
WHERE product_id IN (4, 9);
