WITH with_latest AS (
    SELECT
        s.*,
        latest.transaction_amount,
        latest.transaction_txn_time
    FROM accounts s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount, c.txn_time AS transaction_txn_time
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    account_type,
    customer_id,
    COUNT_IF(account_type = 'CHECKING') AS checking_events,
    COUNT_IF(status = 'OPEN') AS open_events,
    ROUND(AVG(balance), 2) AS avg_balance
FROM with_latest
GROUP BY account_type, customer_id
HAVING checking_events > 1
ORDER BY checking_events DESC NULLS LAST
