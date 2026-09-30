WITH numbered AS (
    SELECT
        id,
        store_id,
        quantity,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS pos
    FROM inventory
    WHERE store_id IN (1, 3, 5)
)
SELECT *
FROM numbered
WHERE pos <= 3
