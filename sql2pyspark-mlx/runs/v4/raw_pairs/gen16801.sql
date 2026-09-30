SELECT product_id, AVG(units) AS avg_units, SUM(units) AS total_units FROM store_sales WHERE revenue < 3200 GROUP BY product_id;
