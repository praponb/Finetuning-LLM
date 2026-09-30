WITH matched_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM inventory
    EXCEPT DISTINCT
    SELECT product_id, id
    FROM inventory
    WHERE product_id = 5
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    INNER JOIN matched_keys k ON s.product_id = k.product_id AND s.id = k.id
)
SELECT
    s.product_id,
    s.quantity,
    (SELECT AVG(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS avg_inventory_quantity
FROM kept_rows s;
