SELECT product_id, MAX(revenue) - MIN(revenue) AS revenue_range, COUNT(units) AS units_count FROM store_sales WHERE store_id IN (2, 3) GROUP BY product_id HAVING SUM(units) > 140
