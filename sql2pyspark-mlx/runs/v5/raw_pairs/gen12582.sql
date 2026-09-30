SELECT
    id,
    store_id,
    revenue,
    product_id
FROM (
    SELECT
        id,
        store_id,
        revenue,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue, id) pos
    FROM store_sales
) r
WHERE pos <= 2
