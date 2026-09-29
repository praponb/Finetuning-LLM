SELECT
    id,
    store_name,
    sqft,
    CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END AS is_tokyo,
    LENGTH(store_name) AS store_name_length
FROM stores
WHERE (store_name IS NOT NULL AND sqft >= 6692) OR city <> 'Bangkok';
