SELECT * FROM (SELECT store_id, product_id, AVG(revenue) AS avg_revenue FROM store_sales GROUP BY store_id, product_id) AS sub WHERE avg_revenue > 1600 ORDER BY product_id DESC, store_id LIMIT 5
