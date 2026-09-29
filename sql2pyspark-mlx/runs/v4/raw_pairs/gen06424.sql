WITH with_last AS (
    SELECT
        s.*,
        x.merchant,
        x.amount
    FROM accounts s
    JOIN LATERAL (
            SELECT c.merchant, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.balance,
    s.opened_date,
    s.status,
    s.merchant,
    s.amount,
    (SELECT MIN(x.shipping_fee) FROM orders x WHERE x.customer_id = s.customer_id) AS min_order_shipping_fee
FROM with_last s
ORDER BY balance DESC;
