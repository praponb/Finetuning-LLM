SELECT id, sqft FROM stores WHERE sqft > (SELECT AVG(sqft) FROM stores);
