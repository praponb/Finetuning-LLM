SELECT product_id, COUNT(DISTINCT units) AS unique_products
FROM store_sales
WHERE YEAR(sale_date) >= 2024
GROUP BY product_id;
