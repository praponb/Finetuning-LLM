SELECT id, city, sqft, store_name FROM (SELECT *, RANK() OVER (PARTITION BY city ORDER BY sqft DESC) AS pos FROM stores) AS r WHERE pos <= 3;
