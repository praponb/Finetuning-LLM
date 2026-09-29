WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        CEIL(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock BETWEEN '2024-02-01' AND '2024-11-16'
)
SELECT product_id, id
FROM valid
EXCEPT DISTINCT
SELECT product_id, id
FROM inventory
WHERE store_id IN (2, 4)
