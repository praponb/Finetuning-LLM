SELECT region, city, COUNT(*) AS num_stores FROM stores WHERE city = 'London' AND sqft > 12841 GROUP BY ROLLUP(region, city)
