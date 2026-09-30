SELECT id, product_id, sale_date
FROM store_sales
WHERE product_id = 9
UNION
SELECT id, product_id, sale_date
FROM store_sales
WHERE units >= 20;
