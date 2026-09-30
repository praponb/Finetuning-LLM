SELECT product_id, ROUND(STDDEV(revenue), 2) AS stddev_revenue, COUNT(*) AS num_store_sales
FROM store_sales
GROUP BY product_id
