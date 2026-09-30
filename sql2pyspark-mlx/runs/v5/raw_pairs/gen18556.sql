SELECT region, YEAR(opened_date) AS opened_year, COUNT(sqft) AS sqft_count, ROUND(STDDEV(sqft), 2) AS stddev_sqft FROM stores GROUP BY region, opened_year ORDER BY 4, sqft_count
