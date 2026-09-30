SELECT *
FROM (
    SELECT HOUR(txn_time) AS txn_hour, SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END) AS shell_count, MIN(amount) AS min_amount
    FROM transactions
    GROUP BY HOUR(txn_time)
) AS t
WHERE min_amount > 2700
