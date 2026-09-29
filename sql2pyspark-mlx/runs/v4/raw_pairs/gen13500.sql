WITH remaining_keys AS (
    -- Set difference on keys
    SELECT product_id
    FROM inventory
    EXCEPT
    SELECT product_id
    FROM inventory
    WHERE product_id IN (1, 3, 5, 10)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    COUNT(*) AS cnt,
    BIT_AND(quantity) AS quantity_bit_and
FROM kept_rows
GROUP BY product_id
ORDER BY product_id DESC NULLS LAST
