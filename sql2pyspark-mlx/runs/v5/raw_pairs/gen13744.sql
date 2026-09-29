SELECT product_id, COUNT(revenue) AS revenue_count, AVG(units) AS avg_units FROM store_sales GROUP BY product_id;
