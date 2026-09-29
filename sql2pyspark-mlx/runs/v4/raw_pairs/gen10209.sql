SELECT store_id, MONTH(sale_date) AS sale_month, ROUND(AVG(units), 2) AS avg_units FROM store_sales GROUP BY store_id, MONTH(sale_date) HAVING ROUND(AVG(units), 2) >= 64;
