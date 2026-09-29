SELECT * FROM (SELECT region, SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END) AS south_count FROM stores GROUP BY region) AS agg WHERE south_count > 80;
