SELECT YEAR(sale_date) AS sale_year, ROUND(STDDEV(revenue), 2) AS stddev_revenue, MIN(sale_date) AS min_sale_date FROM store_sales WHERE store_id IN (1, 3) GROUP BY YEAR(sale_date);
