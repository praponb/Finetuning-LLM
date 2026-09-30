SELECT *
FROM (
    SELECT MONTH(txn_time) AS txn_month, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list, ROUND((100 * SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_credit
    FROM transactions
    GROUP BY MONTH(txn_time)
) AS x
WHERE pct_credit < 56
ORDER BY txn_month;
