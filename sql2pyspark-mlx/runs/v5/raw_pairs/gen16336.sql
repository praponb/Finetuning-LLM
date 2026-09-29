SELECT city, south_count FROM (SELECT city, COUNT(CASE WHEN region = 'South' THEN 1 END) AS south_count FROM stores GROUP BY city) AS t WHERE south_count > 5
