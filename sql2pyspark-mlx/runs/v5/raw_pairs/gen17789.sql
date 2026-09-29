WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT id, store_id
    FROM inventory
    INTERSECT ALL
    SELECT id, store_id
    FROM inventory
    WHERE store_id IN (1, 3, 5, 6)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.store_id = k.store_id
)
SELECT
    s.store_id,
    s.last_restock,
    (SELECT SUM(t2.quantity) FROM inventory t2 WHERE t2.store_id = s.store_id) AS sum_inventory_quantity
FROM kept_rows s
ORDER BY sum_inventory_quantity DESC NULLS LAST
