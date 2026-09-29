SELECT COUNT(*) AS total_count, COUNT(DISTINCT units) AS unique_products
FROM store_sales
WHERE revenue BETWEEN 3300 AND 3900 AND YEAR(sale_date) = 2025
ORDER BY unique_products DESC;
