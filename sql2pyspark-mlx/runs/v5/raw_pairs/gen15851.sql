SELECT
    sqft,
    store_name,
    opened_date,
    COALESCE(store_name, 'Unknown') AS store_name_filled
FROM stores
WHERE region = 'South';
