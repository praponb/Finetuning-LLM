SELECT sale_date, revenue
FROM store_sales
WHERE product_id IN (1, 7) OR product_id IN (4, 6, 10)
ORDER BY sale_date;
