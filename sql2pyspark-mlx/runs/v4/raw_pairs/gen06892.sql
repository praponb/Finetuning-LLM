SELECT MONTH(sale_date) AS sale_month, COUNT(*) AS store_sale_count, COUNT(DISTINCT units) AS unique_products, MAX(units) AS max_units FROM store_sales GROUP BY 1 ORDER BY sale_month DESC, 4 DESC;
