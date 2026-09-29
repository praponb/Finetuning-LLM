SELECT region, avg_sqft, min_sqft
FROM (
    SELECT region, AVG(sqft) AS avg_sqft, MIN(sqft) AS min_sqft
    FROM stores
    GROUP BY region
) AS s
WHERE min_sqft > 6500;
