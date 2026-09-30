SELECT
    a.id,
    a.account_type,
    t.txn_type_list,
    t.amount_count
FROM accounts a
INNER JOIN (SELECT account_id, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list, COUNT(amount) AS amount_count
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.amount_count >= 6
ORDER BY t.amount_count DESC, id DESC
LIMIT 3;
