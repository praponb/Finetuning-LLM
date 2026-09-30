SELECT
    id,
    opened_date,
    region,
    store_name,
    LENGTH(region) AS region_length
FROM stores
WHERE region = 'South' AND UPPER(store_name) LIKE '%ALL%' AND region IN ('Central', 'East', 'West')
ORDER BY region
