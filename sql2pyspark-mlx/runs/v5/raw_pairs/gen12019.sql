SELECT id, region, sqft FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS pos FROM stores) AS r WHERE pos <= 2;
