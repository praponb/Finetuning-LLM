SELECT MIN(sale_date) AS min_sale_date
FROM store_sales
WHERE store_id IN (1, 6);
