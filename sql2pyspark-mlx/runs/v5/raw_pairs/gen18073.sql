SELECT id, product_id, units
FROM (
    SELECT
        id,
        product_id,
        units,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS row_num
    FROM store_sales
) AS t
WHERE row_num <= 5;
