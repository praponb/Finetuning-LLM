SELECT
    id,
    region,
    sqft,
    city,
    MIN(sqft) OVER (PARTITION BY region) AS region_min_sqft,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS rn
FROM stores;
