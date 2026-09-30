SELECT * FROM (SELECT city, region, sqft FROM stores WHERE store_name IS NULL)
PIVOT (SUM(sqft) FOR region IN ('Central', 'South', 'North', 'East'));
