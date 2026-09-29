SELECT * FROM (SELECT city, region, sqft FROM stores)
PIVOT (SUM(sqft) FOR region IN ('Central', 'East', 'South'))
ORDER BY city;
