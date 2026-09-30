WITH with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.transaction_txn_type,
        top1.amount
    FROM accounts s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    account_type,
    balance,
    opened_date,
    status,
    transaction_txn_type,
    amount
FROM with_last
ORDER BY status NULLS FIRST;
