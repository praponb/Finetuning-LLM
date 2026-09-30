SELECT city, COUNT(sqft) AS sqft_count FROM stores GROUP BY 1 ORDER BY sqft_count DESC, city
