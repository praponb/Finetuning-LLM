WITH latest AS (
    SELECT *
    FROM orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel, customer_id ORDER BY shipping_fee DESC, id ASC) = 1
)
SELECT
    id,
    channel,
    discount,
    shipping_fee,
    CASE WHEN channel RLIKE '^(store|mobile)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS channel_zone
FROM latest
ORDER BY channel ASC;
