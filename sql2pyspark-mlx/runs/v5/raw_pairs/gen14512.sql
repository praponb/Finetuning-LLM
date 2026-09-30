WITH with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.channel,
        s.amount,
        s.discount,
        s.shipping_fee,
        latest.method,
        latest.payment_status
    FROM orders s
    JOIN LATERAL (
            SELECT c.method, c.status AS payment_status
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.channel,
    s.amount,
    s.discount,
    s.method,
    s.payment_status,
    CASE WHEN channel RLIKE '^(store|mobile)$' THEN 'GROUP_A' WHEN channel RLIKE '^(web|partner)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS channel_zone,
    (SELECT MAX(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS max_account_balance
FROM with_latest s
ORDER BY amount NULLS FIRST
