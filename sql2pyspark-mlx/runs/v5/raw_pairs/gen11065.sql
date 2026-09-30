WITH with_latest AS (
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.opened_date,
        latest.merchant,
        latest.transaction_amount
    FROM accounts s
    JOIN LATERAL (
            SELECT c.merchant, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.merchant,
    s.transaction_amount,
    (SELECT MAX(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS max_account_balance
FROM with_latest s
ORDER BY max_account_balance DESC, account_type
