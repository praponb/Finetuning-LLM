SELECT * FROM (SELECT region, city, sqft FROM stores) PIVOT (SUM(sqft) FOR city IN ('New York', 'Berlin')) ORDER BY region
