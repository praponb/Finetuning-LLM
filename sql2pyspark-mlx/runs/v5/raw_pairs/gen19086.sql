SELECT store_id, sale_date
FROM store_sales
WHERE revenue BETWEEN 1600 AND 2700
UNION
SELECT store_id, sale_date
FROM store_sales
WHERE product_id IN (1, 5, 7, 9)
ORDER BY sale_date;
