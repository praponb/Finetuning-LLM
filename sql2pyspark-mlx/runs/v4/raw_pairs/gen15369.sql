SELECT a.id, a.opened_date, t.txn_type_list
FROM accounts a
INNER JOIN (SELECT account_id, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
    FROM transactions
    WHERE account_id IN (1, 7, 10)
    GROUP BY account_id) t ON a.id = t.account_id
ORDER BY opened_date;
