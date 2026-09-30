SELECT region, sqft FROM stores WHERE store_name LIKE 'Dow%' UNION SELECT region, sqft FROM stores WHERE city IN ('Singapore', 'Berlin', 'New York')
