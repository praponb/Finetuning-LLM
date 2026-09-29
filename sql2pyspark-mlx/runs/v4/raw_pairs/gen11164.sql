SELECT YEAR(opened_date) AS opened_year, region, SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END) AS south_count FROM stores WHERE region = 'East' GROUP BY CUBE(YEAR(opened_date), region)
