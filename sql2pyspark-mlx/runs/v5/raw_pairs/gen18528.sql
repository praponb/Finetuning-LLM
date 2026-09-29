SELECT a.status, t.txn_type_values
FROM accounts a
INNER JOIN (SELECT account_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) txn_type_values
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
ORDER BY status DESC, t.txn_type_values ASC;
