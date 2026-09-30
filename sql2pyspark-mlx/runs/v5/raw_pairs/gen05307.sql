SELECT id, sqft, region, city FROM stores WHERE sqft < (SELECT MIN(sqft) FROM stores WHERE sqft >= 14065)
