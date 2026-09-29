WITH recent_orders AS (
    SELECT *
    FROM orders
    WHERE shipping_fee <= 26
),
orders_stats AS (
    SELECT
        status,
        CASE WHEN discount >= 0.33 THEN 'high' WHEN discount >= 0.1 THEN 'mid' ELSE 'low' END AS discount_bucket,
        AVG(shipping_fee) AS avg_shipping_fee,
        SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
    FROM recent_orders
    GROUP BY status, CASE WHEN discount >= 0.33 THEN 'high' WHEN discount >= 0.1 THEN 'mid' ELSE 'low' END
)
SELECT
    status,
    discount_bucket,
    avg_shipping_fee,
    channel_list
FROM orders_stats
WHERE avg_shipping_fee < 31
ORDER BY avg_shipping_fee DESC
