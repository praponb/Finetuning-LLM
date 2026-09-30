SELECT
    id,
    store_name,
    region,
    city,
    INITCAP(TRIM(store_name)) store_name_title
FROM stores;
