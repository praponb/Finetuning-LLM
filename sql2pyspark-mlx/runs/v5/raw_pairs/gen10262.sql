SELECT
    id,
    account_id,
    txn_time,
    amount
FROM (
    SELECT
        id,
        account_id,
        txn_time,
        amount,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) pos
    FROM transactions
) r
WHERE pos <= 2;
