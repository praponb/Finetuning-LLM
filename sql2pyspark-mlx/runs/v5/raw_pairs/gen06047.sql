WITH with_last AS (
    SELECT
        s.id,
        s.name,
        s.email,
        latest.order_channel
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.channel AS order_channel
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    order_channel,
    SORT_ARRAY(COLLECT_SET(order_channel)) AS order_channel_list,
    COUNT(CASE WHEN order_channel = 'store' THEN 1 END) AS store_count,
    COUNT(*) AS cnt,
    SIZE(COLLECT_SET(order_channel)) AS num_order_channels,
    COUNT(*) AS cnt_2,
    SIZE(COLLECT_SET(order_channel)) AS num_order_channels_2
FROM with_last
GROUP BY order_channel
