SELECT product_id, MAX(units) AS max_units, ROUND(AVG(revenue), 2) AS avg_revenue, MAX(units) - MIN(units) AS units_range FROM store_sales WHERE units <= 25 GROUP BY product_id;
