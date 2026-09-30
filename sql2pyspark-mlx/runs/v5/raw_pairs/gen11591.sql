WITH matched_keys AS (
    -- Set intersection on keys
    SELECT order_id, id
    FROM order_items
    INTERSECT
    SELECT order_id, id
    FROM order_items
    WHERE product_id IN (20, 27, 29)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    INNER JOIN matched_keys k ON s.order_id = k.order_id AND s.id = k.id
)
SELECT
    id,
    order_id,
    product_id,
    quantity
FROM reconciled
ORDER BY quantity NULLS FIRST;
