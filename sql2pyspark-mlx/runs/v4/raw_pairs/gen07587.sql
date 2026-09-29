SELECT store_id, COUNT(revenue) AS revenue_count FROM store_sales WHERE product_id = 7 AND product_id IN (1, 2) GROUP BY store_id ORDER BY store_id;
