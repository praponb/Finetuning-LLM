WITH enriched AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.quantity,
        e.last_restock,
        d.price
    FROM inventory e
    INNER JOIN products d
        ON e.product_id = d.id
)
SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    price
FROM enriched
