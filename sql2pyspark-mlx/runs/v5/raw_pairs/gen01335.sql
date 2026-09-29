SELECT id, sqft FROM stores WHERE sqft > (SELECT MAX(sqft) FROM stores WHERE city IN ('Chiang Mai', 'Tokyo', 'Berlin'))
