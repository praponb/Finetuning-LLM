WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, quantity * unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (1, 2, 14, 32)
)
-- Attach products attributes
SELECT
    src.*,
    p.category,
    p.price
FROM filtered src
LEFT JOIN products p
    ON src.product_id <=> p.id
