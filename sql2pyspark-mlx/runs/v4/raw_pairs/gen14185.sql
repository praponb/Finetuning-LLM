SELECT
    id,
    sqft,
    store_name,
    UPPER(region) AS region_upper
FROM stores
WHERE MONTH(opened_date) IN (3, 7, 8) AND sqft <> 13374
