SELECT * FROM (SELECT city, region, sqft FROM stores) PIVOT (SUM(sqft) FOR region IN ('West', 'North', 'East'))
