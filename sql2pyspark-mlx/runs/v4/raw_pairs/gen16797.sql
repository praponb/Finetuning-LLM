SELECT
    id,
    region,
    sqft,
    SUBSTRING(store_name, 1, 3) store_name_prefix,
    UPPER(region) region_upper
FROM stores;
