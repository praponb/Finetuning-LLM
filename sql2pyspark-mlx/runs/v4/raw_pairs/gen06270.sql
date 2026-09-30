SELECT
    id,
    region,
    sqft,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS rn,
    SUM(sqft) OVER (PARTITION BY region) AS region_sum_sqft
FROM stores
WHERE LENGTH(store_name) > 6;
