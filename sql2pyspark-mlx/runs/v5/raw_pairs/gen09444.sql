SELECT MAX(sale_date) latest_sale_date
FROM store_sales
WHERE store_id IN (1, 5) AND sale_date BETWEEN '2023-04-01' AND '2025-06-05' AND sale_date > '2024-07-01';
