SELECT id, sqft, region
FROM stores
WHERE sqft > (SELECT AVG(sqft) FROM stores WHERE region IS NULL)
