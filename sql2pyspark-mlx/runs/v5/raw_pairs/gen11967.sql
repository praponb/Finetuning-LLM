SELECT * FROM (SELECT city, region, sqft FROM stores) PIVOT (AVG(sqft) FOR region IN ('North', 'Central'));
