SELECT *
FROM (
    SELECT merchant, MAX(txn_time) AS latest_txn_time, SUM(CASE WHEN merchant = 'Shell' THEN amount ELSE 0 END) AS shell_amount
    FROM transactions
    GROUP BY merchant
) AS s
WHERE shell_amount < 29
ORDER BY shell_amount DESC, merchant DESC
LIMIT 50;
