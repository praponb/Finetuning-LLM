WITH order_items_by_order AS (
    SELECT order_id, MAX(unit_price) max_unit_price, MAX(quantity) - MIN(quantity) quantity_range
    FROM order_items
    WHERE quantity BETWEEN 4 AND 5
    GROUP BY order_id
)
SELECT
    o.id,
    o.discount,
    o.shipping_fee,
    oibo.max_unit_price,
    oibo.quantity_range
FROM orders o
JOIN order_items_by_order oibo ON o.id = oibo.order_id;
