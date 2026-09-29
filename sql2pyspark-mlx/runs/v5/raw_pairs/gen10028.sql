WITH order_summary AS (
    SELECT order_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) method_values, MIN(amount) min_amount
    FROM payments
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.shipping_fee,
    order_summary.method_values,
    order_summary.min_amount
FROM orders
LEFT JOIN order_summary ON orders.id = order_summary.order_id
WHERE order_summary.min_amount > 1470
ORDER BY shipping_fee DESC, orders.id DESC
