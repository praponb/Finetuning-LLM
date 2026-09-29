SELECT MAX(sqft) AS max_sqft
FROM stores
WHERE sqft < 15942
ORDER BY max_sqft;
