SELECT
    carrier,
    order_id,
    unique_orders,
    status_values
FROM (
    SELECT
        carrier,
        order_id,
        COUNT(DISTINCT cost) AS unique_orders,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM shipments
    WHERE MONTH(shipped_date) IN (3, 10, 12)
    GROUP BY carrier, order_id
) AS sub
WHERE unique_orders >= 4
