SELECT region, COUNT(*) AS cnt, COUNT(CASE WHEN region = 'West' THEN 1 END) AS west_count FROM stores WHERE (region != 'North' AND MONTH(opened_date) IN (5, 7, 9)) OR sqft != 14293 GROUP BY region;
