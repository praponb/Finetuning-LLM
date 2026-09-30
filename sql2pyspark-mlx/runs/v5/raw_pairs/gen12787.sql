SELECT *
FROM (
    SELECT QUARTER(txn_time) AS txn_quarter, SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END) AS shell_count
    FROM transactions
    GROUP BY QUARTER(txn_time)
) AS agg
WHERE shell_count >= 40
ORDER BY txn_quarter;
