WITH account_summary AS (
    SELECT account_id, MAX(txn_time) AS last_txn_time, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
    FROM transactions
    WHERE txn_type NOT IN ('CREDIT', 'DEBIT', 'FEE')
    GROUP BY account_id
)
SELECT a.opened_date, as.last_txn_time, as.merchant_list
FROM accounts a
LEFT JOIN account_summary as ON a.id = as.account_id
ORDER BY opened_date ASC
