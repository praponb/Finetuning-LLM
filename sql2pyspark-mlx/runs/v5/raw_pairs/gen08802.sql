WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, unit_price - quantity AS line_total
    FROM order_items
    WHERE quantity < 4
)
SELECT order_id, product_id
FROM valid
INTERSECT ALL
SELECT order_id, product_id
FROM order_items
WHERE order_id = 3;
