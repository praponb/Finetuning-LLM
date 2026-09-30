SELECT id, sqft, ROUND(sqft, 0) AS sqft_rounded
FROM stores
WHERE city IN ('London', 'Singapore') AND LENGTH(store_name) >= 13;
