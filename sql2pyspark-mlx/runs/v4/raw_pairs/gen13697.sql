SELECT
    id,
    opened_date,
    region,
    sqft,
    LENGTH(store_name) AS store_name_length
FROM stores
WHERE city IN ('Chiang Mai', 'London') AND city IN ('Chiang Mai', 'Tokyo')
