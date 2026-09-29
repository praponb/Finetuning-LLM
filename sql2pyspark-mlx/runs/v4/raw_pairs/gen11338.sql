SELECT YEAR(sale_date) sale_year, MAX(sale_date) last_sale_date, MIN(sale_date) first_sale_date FROM store_sales GROUP BY sale_year ORDER BY sale_year LIMIT 5
