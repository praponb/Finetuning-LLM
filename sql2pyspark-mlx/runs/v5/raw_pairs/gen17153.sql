SELECT id, sale_date, store_id
FROM store_sales
WHERE product_id IN (3, 4, 7) AND sale_date < '2025-10-16' AND YEAR(sale_date) = 2024
ORDER BY store_id;
