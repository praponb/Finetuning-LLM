SELECT product_id, COUNT(DISTINCT revenue) AS unique_stores, MIN(units) AS min_units, COUNT(revenue) AS revenue_count FROM store_sales GROUP BY product_id ORDER BY min_units DESC, product_id LIMIT 1;
