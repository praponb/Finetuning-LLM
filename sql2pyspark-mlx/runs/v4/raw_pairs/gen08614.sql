SELECT * FROM (SELECT id, region, sqft, city, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS rn FROM stores) AS t WHERE rn <= 5
