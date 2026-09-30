SELECT
    id,
    city,
    region,
    sqft,
    NTILE(3) OVER (PARTITION BY city, region ORDER BY sqft, id) AS bucket
FROM stores
WHERE city = 'Singapore';
