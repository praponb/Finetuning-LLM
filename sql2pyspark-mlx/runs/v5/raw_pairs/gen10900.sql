SELECT AVG(units) AS avg_units, SUM(revenue) AS total_revenue, ROUND(STDDEV(units), 2) AS stddev_units FROM store_sales WHERE product_id IN (5, 8, 9, 10)
