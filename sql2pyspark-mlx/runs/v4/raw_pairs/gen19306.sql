WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, channel ORDER BY amount DESC, id) AS row_num
    FROM orders
    QUALIFY row_num = 1
)
SELECT
    id,
    customer_id,
    channel,
    amount,
    discount,
    shipping_fee,
    CASE WHEN channel RLIKE '^(web|partner)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS channel_zone
FROM ranked
ORDER BY shipping_fee DESC;
