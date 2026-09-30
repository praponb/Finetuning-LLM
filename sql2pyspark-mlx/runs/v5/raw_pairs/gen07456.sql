WITH ranked_store_sales AS (
    SELECT
        id,
        product_id,
        units,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS pos
    FROM store_sales
)
SELECT id, product_id, units
FROM ranked_store_sales
WHERE pos <= 1
ORDER BY product_id DESC, id DESC
