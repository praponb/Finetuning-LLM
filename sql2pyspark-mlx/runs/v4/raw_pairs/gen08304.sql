SELECT
    id,
    city,
    sqft,
    store_name
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) row_num
    FROM stores
) t
WHERE row_num <= 5
