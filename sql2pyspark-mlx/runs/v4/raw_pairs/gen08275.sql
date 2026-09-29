SELECT ROUND(AVG(revenue), 2) AS avg_revenue, MAX(sale_date) AS last_sale_date, MAX(sale_date) AS latest_sale_date
FROM store_sales
ORDER BY avg_revenue DESC, last_sale_date
