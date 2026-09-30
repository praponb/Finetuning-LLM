WITH deduped AS (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
    FROM orders
    QUALIFY rn = 1
)
SELECT
    id,
    store_id,
    order_date,
    channel,
    amount,
    discount,
    rn,
    CASE WHEN channel RLIKE '^(partner|web)$' THEN 'PRIORITY' WHEN channel RLIKE '^(mobile|store)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS channel_zone
FROM deduped
ORDER BY channel_zone ASC, amount DESC;
