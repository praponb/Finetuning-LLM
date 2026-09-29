WITH order_stats AS (
    SELECT order_id, MIN(shipped_date) AS first_shipped_date, SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.status,
    o.store_id,
    os.first_shipped_date,
    os.carrier_list
FROM orders o
INNER JOIN order_stats os ON o.id = os.order_id
ORDER BY store_id DESC;
