WITH ranked_transactions AS (
    SELECT
        id,
        account_id,
        txn_time,
        is_flagged,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) AS row_num
    FROM transactions
)
SELECT
    id,
    account_id,
    txn_time,
    is_flagged
FROM ranked_transactions
WHERE row_num <= 3
ORDER BY account_id DESC
