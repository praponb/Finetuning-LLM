SELECT
    id,
    city,
    sqft,
    store_name
FROM (
    SELECT
        id,
        city,
        sqft,
        store_name,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) rn
    FROM stores
    WHERE sqft > 9312
) ranked
WHERE rn = 1;
