WITH latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.status,
        top1.merchant
    FROM accounts s
    JOIN LATERAL (
            SELECT c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.balance,
    s.status,
    s.merchant,
    (SELECT MIN(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS min_order_amount
FROM latest_match s
ORDER BY merchant ASC, id ASC
