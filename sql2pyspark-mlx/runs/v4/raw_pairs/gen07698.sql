SELECT
    orders.status,
    QUARTER(orders.order_date) AS order_quarter,
    COUNT(DISTINCT shipments.cost) AS unique_orders,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(shipments.carrier))) AS carrier_values
FROM shipments
JOIN orders ON shipments.order_id = orders.id
GROUP BY orders.status, QUARTER(orders.order_date)
HAVING COUNT(DISTINCT shipments.cost) > 4
ORDER BY 3, order_quarter
