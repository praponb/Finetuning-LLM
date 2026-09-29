SELECT * FROM (SELECT id, city, sqft, region, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS row_num FROM stores) AS r WHERE row_num <= 2 ORDER BY city, sqft DESC;
