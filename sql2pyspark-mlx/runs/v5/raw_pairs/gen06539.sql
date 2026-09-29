WITH order_totals AS (
    SELECT order_id, COUNT(DISTINCT unit_price) AS unique_orders
    FROM order_items
    WHERE order_id IN (8, 39)
    GROUP BY order_id
)
SELECT
    o.id,
    o.amount,
    o.store_id,
    ot.unique_orders
FROM orders o
INNER JOIN order_totals ot ON o.id = ot.order_id
ORDER BY o.id DESC;
