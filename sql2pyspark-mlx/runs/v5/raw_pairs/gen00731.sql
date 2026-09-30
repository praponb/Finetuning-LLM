WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(unit_price, quantity) AS least_unit_price_quantity
    FROM order_items
    WHERE quantity < 6
)
SELECT id, product_id
FROM base
INTERSECT
SELECT id, product_id
FROM order_items
WHERE product_id = 14
