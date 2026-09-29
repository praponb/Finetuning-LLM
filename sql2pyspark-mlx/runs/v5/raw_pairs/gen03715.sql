SELECT
    id,
    merchant,
    txn_time,
    NTILE(10) OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS quartile
FROM transactions
ORDER BY id DESC;
