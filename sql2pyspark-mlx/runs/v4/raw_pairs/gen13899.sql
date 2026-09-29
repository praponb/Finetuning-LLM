WITH remaining_keys AS (
    SELECT product_id, id
    FROM inventory
    INTERSECT ALL
    SELECT product_id, id
    FROM inventory
    WHERE product_id IN (2, 4, 6, 8)
),
kept_rows AS (
    SELECT s.*
    FROM inventory s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.id = k.id
)
SELECT
    id,
    product_id,
    quantity,
    last_restock
FROM kept_rows
