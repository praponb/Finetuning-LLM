WITH shipments_by_order AS (
    SELECT order_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values, MAX(cost) max_cost
    FROM shipments
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.status,
    orders.customer_id,
    shipments_by_order.status_values,
    shipments_by_order.max_cost
FROM orders
INNER JOIN shipments_by_order ON orders.id = shipments_by_order.order_id
ORDER BY orders.status DESC
