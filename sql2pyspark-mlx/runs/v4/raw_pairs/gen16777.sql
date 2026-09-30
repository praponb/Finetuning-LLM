WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS pos
    FROM inventory
)
SELECT
    id,
    product_id,
    quantity,
    store_id
FROM numbered
WHERE pos <= 3;
