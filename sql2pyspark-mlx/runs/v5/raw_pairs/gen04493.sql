SELECT
    id,
    city,
    region,
    store_name,
    UPPER(store_name) AS store_name_upper
FROM stores
WHERE region IN ('West', 'North') AND city IN ('London', 'Bangkok', 'Tokyo') AND sqft > 9722
