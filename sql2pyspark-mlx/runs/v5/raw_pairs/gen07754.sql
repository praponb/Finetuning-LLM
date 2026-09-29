SELECT id
FROM store_sales
EXCEPT DISTINCT
SELECT id
FROM store_sales
WHERE product_id = 3;
