SELECT product_id, SUM(revenue) AS total_revenue FROM store_sales GROUP BY product_id ORDER BY total_revenue DESC;
