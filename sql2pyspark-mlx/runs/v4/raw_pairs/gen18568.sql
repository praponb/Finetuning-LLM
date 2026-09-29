SELECT id, sqft FROM stores WHERE sqft >= (SELECT MAX(sqft) FROM stores) ORDER BY id;
