WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST, id) AS rn
    FROM inventory
    QUALIFY rn = 1
)
SELECT
    id,
    quantity,
    last_restock,
    rn
FROM deduped
ORDER BY id;
