SELECT
    id,
    product_id,
    units,
    revenue
FROM (
    SELECT
        id,
        product_id,
        units,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS pos
    FROM store_sales
) AS r
WHERE pos <= 3;
