SELECT id, sqft, store_name, city FROM stores WHERE sqft >= (SELECT AVG(sqft) FROM stores);
