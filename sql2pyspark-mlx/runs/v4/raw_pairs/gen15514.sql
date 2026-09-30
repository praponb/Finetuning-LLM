SELECT store_id, ROUND(STDDEV(revenue), 2) stddev_revenue, COUNT(revenue) revenue_count FROM store_sales WHERE product_id IN (2, 4, 6) GROUP BY store_id ORDER BY revenue_count DESC, store_id
