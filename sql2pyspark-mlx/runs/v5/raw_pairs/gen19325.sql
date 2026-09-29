SELECT id, sqft, region FROM stores WHERE sqft > (SELECT AVG(sqft) FROM stores WHERE sqft > 10533) ORDER BY region ASC
