SELECT accounts.balance, t.num_transactions, t.txn_type_list
FROM accounts
LEFT JOIN (SELECT account_id, COUNT(*) AS num_transactions, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
    FROM transactions
    GROUP BY account_id) t ON accounts.id = t.account_id
WHERE t.num_transactions >= 10
ORDER BY num_transactions;
