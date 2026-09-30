SELECT * FROM (SELECT region, city, sqft FROM stores) PIVOT (SUM(sqft) FOR city IN ('Berlin', 'London', 'New York', 'Bangkok'));
