SELECT product_id
FROM store_sales
WHERE MONTH(sale_date) IN (2, 6, 10)
UNION
SELECT product_id
FROM store_sales
WHERE sale_date <= '2025-03-22'
ORDER BY product_id;
