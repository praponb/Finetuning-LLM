SELECT store_id, SUM(revenue) AS total_revenue, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(revenue) DESC) AS rnk FROM store_sales WHERE units BETWEEN 14 AND 40 GROUP BY store_id
