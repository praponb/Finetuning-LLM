SELECT MONTH(opened_date) AS opened_month, COUNT(sqft) AS sqft_count FROM stores WHERE region = 'Central' GROUP BY opened_month ORDER BY sqft_count DESC;
