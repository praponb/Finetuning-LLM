WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        ROUND(quantity, 0) AS quantity_rounded
    FROM inventory
    WHERE last_restock >= DATE '2023-09-28'
)
-- Attach products attributes
SELECT
    a.id,
    a.store_id,
    a.product_id,
    a.quantity_rounded,
    d.price
FROM filtered a
LEFT JOIN products d
    ON a.product_id = d.id
