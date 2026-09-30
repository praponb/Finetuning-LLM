WITH AccountSummary AS (
    SELECT account_id, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
    FROM transactions
    GROUP BY account_id
)
SELECT a.account_type, A.txn_type_list
FROM accounts a
JOIN AccountSummary A ON a.id = A.account_id
ORDER BY a.account_type;
