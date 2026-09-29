SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY txn_time DESC) AS pos
    FROM transactions
) AS r
WHERE pos <= 3
ORDER BY account_id DESC;
