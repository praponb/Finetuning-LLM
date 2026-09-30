SELECT store_id, MAX(sale_date) AS last_sale_date
FROM store_sales
WHERE store_id IN (1, 3, 5)
GROUP BY store_id
