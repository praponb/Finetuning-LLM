SELECT
    id,
    region,
    sqft,
    MAX(sqft) OVER (PARTITION BY region) AS region_max_sqft
FROM stores
WHERE MONTH(opened_date) IN (2, 3, 6)
ORDER BY id DESC
