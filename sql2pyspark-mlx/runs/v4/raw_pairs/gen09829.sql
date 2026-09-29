WITH filtered_orders AS (
    SELECT *, ROUND(amount, 0) amount_rounded
    FROM orders
    WHERE store_id = 8 AND store_id IS NOT NULL
),
orders_stats AS (
    SELECT channel, ROUND(AVG(amount), 2) avg_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values
    FROM filtered_orders
    GROUP BY channel
)
SELECT channel, avg_amount, status_values
FROM orders_stats
ORDER BY avg_amount DESC, channel
LIMIT 3;
