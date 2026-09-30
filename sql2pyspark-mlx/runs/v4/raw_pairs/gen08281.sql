SELECT product_id, SUM(revenue) AS total_revenue
FROM store_sales
WHERE store_id IN (1, 2, 4, 6)
GROUP BY product_id
