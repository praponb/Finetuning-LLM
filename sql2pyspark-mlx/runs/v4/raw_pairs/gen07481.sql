SELECT product_id, SUM(units) AS total_units, RANK() OVER (ORDER BY SUM(units) DESC) AS units_rank FROM store_sales WHERE revenue < 2000 GROUP BY product_id ORDER BY units_rank DESC;
