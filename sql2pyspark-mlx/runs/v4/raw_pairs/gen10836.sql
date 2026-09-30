SELECT region, ROUND(AVG(sqft), 2) AS avg_sqft, MAX(sqft) AS max_sqft FROM stores WHERE store_name LIKE '%Expre%' GROUP BY region;
