SELECT
    src.id,
    src.account_id,
    src.amount,
    src.txn_time,
    src.merchant,
    d.status
FROM transactions src
LEFT JOIN accounts d
    ON src.account_id = d.id
