SELECT
    id,
    merchant,
    txn_time,
    amount
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS pos
    FROM transactions
) AS ranked
WHERE pos = 1
ORDER BY merchant ASC;
