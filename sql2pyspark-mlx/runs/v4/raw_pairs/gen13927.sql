WITH with_last AS (
    SELECT
        s.*,
        x.payment_status,
        x.paid_at
    FROM orders s
    LEFT JOIN LATERAL (
            SELECT c.status AS payment_status, c.paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.channel,
    s.amount,
    s.shipping_fee,
    s.paid_at,
    CASE WHEN channel RLIKE '^(store|web)$' THEN 'EXTENDED' WHEN channel RLIKE '^(mobile|partner)$' THEN 'CORE' ELSE 'PRIORITY' END AS channel_zone,
    (SELECT AVG(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM with_last s
ORDER BY amount DESC NULLS LAST
