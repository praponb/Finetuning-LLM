WITH valid_transactions AS (
    SELECT *
    FROM transactions
    WHERE is_flagged = FALSE OR account_id IN (1, 8, 20, 21)
),
agg AS (
    SELECT YEAR(txn_time) AS txn_year, CASE WHEN txn_type IN ('DEBIT', 'CREDIT') THEN 'priority' ELSE 'other' END AS txn_type_group, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
    FROM valid_transactions
    GROUP BY YEAR(txn_time), CASE WHEN txn_type IN ('DEBIT', 'CREDIT') THEN 'priority' ELSE 'other' END
)
SELECT txn_year, txn_type_group
FROM agg
ORDER BY txn_year DESC;
