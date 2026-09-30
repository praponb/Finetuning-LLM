WITH OrderSummary AS (
    SELECT order_id, SUM(quantity) AS total_quantity, SUM(unit_price) AS total_unit_price
    FROM order_items
    WHERE order_id = 27
    GROUP BY order_id
)
SELECT
    o.id,
    o.status,
    o.store_id,
    O.total_quantity,
    O.total_unit_price
FROM orders o
JOIN OrderSummary O ON o.id = O.order_id
WHERE O.total_unit_price >= 1900
ORDER BY o.store_id DESC
