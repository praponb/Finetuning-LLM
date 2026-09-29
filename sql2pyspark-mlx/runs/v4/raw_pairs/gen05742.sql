WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY order_date DESC, id) AS rn
        FROM orders
    ) x
    WHERE rn = 1
)
SELECT
    id,
    channel,
    amount,
    shipping_fee,
    rn,
    CASE WHEN channel RLIKE '^(web|store)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS channel_zone
FROM ranked;
