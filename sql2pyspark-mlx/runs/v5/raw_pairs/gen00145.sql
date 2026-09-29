WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price + quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN unit_price >= 340 THEN 'High' WHEN unit_price >= 180 THEN 'Medium' ELSE 'Low' END AS unit_price_band
    FROM filtered
    WHERE product_id IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT order_id, product_id
    FROM valid
    INTERSECT ALL
    SELECT order_id, product_id
    FROM order_items
    WHERE quantity > 7
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.product_id = k.product_id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        unit_price,
        unit_price_band,
        COALESCE(quantity, 0) AS quantity_filled,
        quantity + line_total AS quantity_plus_line_total
    FROM kept_rows
    WHERE product_id IS NOT NULL
      AND order_id = 30
)
SELECT
    id,
    quantity_filled,
    quantity_plus_line_total
FROM cleaned
ORDER BY id DESC NULLS FIRST;
