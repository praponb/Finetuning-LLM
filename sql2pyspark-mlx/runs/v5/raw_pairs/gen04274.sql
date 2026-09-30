SELECT store_id
FROM store_sales
WHERE revenue > 2900
EXCEPT
SELECT store_id
FROM store_sales
WHERE sale_date <= '2024-08-01';
