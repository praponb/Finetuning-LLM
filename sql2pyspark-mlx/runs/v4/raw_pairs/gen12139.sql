SELECT store_id, ROUND(STDDEV(units), 2) AS stddev_units FROM store_sales GROUP BY store_id HAVING ROUND(STDDEV(units), 2) < 38;
