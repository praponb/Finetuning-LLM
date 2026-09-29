WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, store_id
    FROM inventory
    EXCEPT DISTINCT
    SELECT id, store_id
    FROM inventory
    WHERE last_restock > '2023-03-23'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    INNER JOIN kept_keys k ON s.id = k.id AND s.store_id = k.store_id
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.quantity,
    s.last_restock,
    (SELECT AVG(t2.units) FROM store_sales t2 WHERE t2.store_id = s.store_id) AS avg_store_sale_units
FROM verified s
ORDER BY store_id DESC NULLS FIRST;
