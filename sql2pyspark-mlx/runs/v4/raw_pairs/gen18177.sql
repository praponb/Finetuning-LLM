SELECT ss.id, ss.sale_date, ss.store_id
FROM store_sales ss
WHERE ss.product_id IN (SELECT id FROM products WHERE YEAR(launch_date) = 2023)
ORDER BY sale_date;
