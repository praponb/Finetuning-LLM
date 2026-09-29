SELECT
    id,
    region,
    sqft,
    AVG(sqft) OVER (PARTITION BY region) AS region_avg_sqft
FROM stores;
