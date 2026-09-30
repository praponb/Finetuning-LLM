SELECT product_id, store_id, ROUND(STDDEV(units), 2) stddev_units, ROUND(AVG(units), 2) avg_units FROM store_sales WHERE sale_date <= '2024-05-27' GROUP BY 1, 2 HAVING ROUND(AVG(units), 2) < 41
