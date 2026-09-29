SELECT
    id,
    product_id,
    units,
    store_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units, id) row_num
    FROM store_sales
) t
WHERE row_num <= 3
ORDER BY product_id DESC
