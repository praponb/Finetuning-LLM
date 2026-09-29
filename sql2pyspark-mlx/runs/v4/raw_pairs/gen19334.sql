SELECT MONTH(sale_date) AS sale_month, COUNT(DISTINCT revenue) AS unique_products
FROM store_sales
GROUP BY MONTH(sale_date)
HAVING COUNT(revenue) >= 4
