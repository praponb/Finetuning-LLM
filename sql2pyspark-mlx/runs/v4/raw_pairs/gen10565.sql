SELECT * FROM (SELECT city, region, sqft FROM stores WHERE city = 'New York') PIVOT (MAX(sqft) FOR region IN ('South', 'North', 'Central', 'East')) ORDER BY city
