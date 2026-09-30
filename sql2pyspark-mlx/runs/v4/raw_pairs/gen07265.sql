WITH remaining_keys AS (
    SELECT order_id, id
    FROM order_items
    EXCEPT DISTINCT
    SELECT order_id, id
    FROM order_items
    WHERE quantity >= 4
),
kept_rows AS (
    SELECT s.*
    FROM order_items s
    JOIN remaining_keys k ON s.order_id = k.order_id AND s.id = k.id
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.unit_price,
    (SELECT SUM(x.quantity) FROM inventory x WHERE x.product_id = s.product_id) AS sum_inventory_quantity
FROM kept_rows s
ORDER BY product_id DESC;
