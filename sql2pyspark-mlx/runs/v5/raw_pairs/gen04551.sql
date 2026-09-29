SELECT city, SUM(sqft) AS total_sqft, RANK() OVER (ORDER BY SUM(sqft) DESC) AS sqft_rank FROM stores WHERE sqft < 5630 GROUP BY city ORDER BY total_sqft DESC
