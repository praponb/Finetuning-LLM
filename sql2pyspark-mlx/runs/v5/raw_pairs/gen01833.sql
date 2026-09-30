WITH remaining_keys AS (
    SELECT order_id
    FROM shipments
    INTERSECT ALL
    SELECT order_id
    FROM shipments
    WHERE cost >= 27
),
verified AS (
    SELECT s.*
    FROM shipments s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id
)
SELECT
    order_id,
    COUNT(*) FILTER (WHERE carrier = 'FedEx') AS fedex_rows,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE carrier = 'DHL') AS dhl_rows,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list_2
FROM verified
GROUP BY order_id
ORDER BY event_count
