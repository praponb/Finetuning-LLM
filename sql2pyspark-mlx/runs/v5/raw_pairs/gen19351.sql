SELECT product_id, SUM(revenue) AS total_revenue, COUNT(*) AS num_store_sales
FROM store_sales
GROUP BY product_id;
