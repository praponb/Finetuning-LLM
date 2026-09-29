WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        LEAD(quantity, 1) OVER (PARTITION BY store_id ORDER BY quantity ASC, id ASC) AS next_quantity,
        LEAD(quantity, 1) OVER (ORDER BY last_restock ASC, id ASC) AS next_quantity_2
    FROM inventory
)
SELECT
    id,
    product_id,
    quantity,
    next_quantity,
    next_quantity_2
FROM with_windows
ORDER BY next_quantity DESC
