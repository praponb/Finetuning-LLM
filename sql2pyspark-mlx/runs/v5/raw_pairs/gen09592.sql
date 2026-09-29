WITH joined AS (
    SELECT
        e.*,
        o.channel,
        o.amount AS order_amount,
        o.shipping_fee
    FROM payments e
    LEFT JOIN orders o
        ON e.order_id = o.id
)
SELECT
    id,
    paid_at,
    status,
    channel,
    order_amount,
    shipping_fee,
    CASE WHEN channel RLIKE '^(partner|store)$' THEN 'PRIORITY' ELSE 'CORE' END AS channel_zone
FROM joined
