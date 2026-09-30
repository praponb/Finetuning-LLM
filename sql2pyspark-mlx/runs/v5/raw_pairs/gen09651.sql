SELECT
    id,
    city,
    region,
    MIN(sqft) OVER (PARTITION BY region) AS region_min_sqft,
    FIRST_VALUE(region) IGNORE NULLS OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region
FROM stores;
