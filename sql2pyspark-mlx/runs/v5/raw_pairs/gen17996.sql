SELECT product_id, MIN(units) AS min_units, ROUND(STDDEV(units), 2) AS stddev_units FROM store_sales WHERE store_id IN (1, 6) GROUP BY product_id
