SELECT
    id,
    merchant,
    txn_time,
    account_id
FROM (
    SELECT
        id,
        merchant,
        txn_time,
        account_id,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS row_num
    FROM transactions
    WHERE account_id IN (24, 35, 36, 39)
) AS ranked
WHERE row_num <= 5
ORDER BY merchant;
