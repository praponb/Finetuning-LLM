SELECT *
FROM (
    SELECT
        id,
        product_id,
        units,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS rnk
    FROM store_sales
) AS r
WHERE rnk <= 1
