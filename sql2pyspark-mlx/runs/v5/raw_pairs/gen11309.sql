WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(unit_price, 0) AS unit_price_filled
    FROM order_items
    WHERE quantity BETWEEN 4 AND 8
)
-- Deduplicate with ROW_NUMBER
SELECT
    id,
    product_id,
    unit_price,
    unit_price_filled
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity, id) = 1;
