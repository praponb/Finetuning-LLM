SELECT
    id,
    sqft,
    region,
    store_name
FROM stores
WHERE sqft >= (SELECT AVG(sqft) FROM stores)
ORDER BY sqft, id
LIMIT 3;
