SELECT
    sqft,
    city,
    LOWER(city) AS city_lower,
    LOWER(store_name) AS store_name_lower
FROM stores
WHERE LOWER(TRIM(store_name)) = 'old town'
