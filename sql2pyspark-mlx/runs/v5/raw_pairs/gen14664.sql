SELECT
    id,
    city,
    sqft,
    region
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft DESC) AS pos
    FROM stores
) AS t
WHERE pos <= 5
ORDER BY region;
