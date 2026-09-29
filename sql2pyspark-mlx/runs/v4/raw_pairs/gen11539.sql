SELECT
    id,
    region,
    city,
    sqft,
    opened_date,
    MAX(sqft) OVER (PARTITION BY region, city) AS region_max_sqft,
    LEAD(sqft, 1) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft
FROM stores;
