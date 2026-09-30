SELECT
    s.*,
    x.amount,
    x.transaction_txn_type
FROM accounts s
LEFT JOIN LATERAL (
        SELECT c.amount, c.txn_type AS transaction_txn_type
        FROM transactions c
        WHERE c.account_id = s.id
          AND c.txn_time <= s.opened_date
        ORDER BY c.txn_time DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE;
