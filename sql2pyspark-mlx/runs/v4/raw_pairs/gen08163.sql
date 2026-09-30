SELECT
    id,
    region,
    sqft,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) row_num
FROM stores
ORDER BY sqft, row_num
