SELECT YEAR(opened_date) opened_year, COUNT(DISTINCT sqft) unique_regions, SUM(CASE WHEN region = 'North' THEN 1 ELSE 0 END) north_count, COUNT(sqft) sqft_count FROM stores GROUP BY YEAR(opened_date)
