SELECT
    a.id,
    a.opened_date,
    t.amazon_count,
    t.txn_type_list
FROM accounts a
JOIN (SELECT account_id, SUM(CASE WHEN merchant = 'Amazon' THEN 1 ELSE 0 END) amazon_count, SORT_ARRAY(COLLECT_SET(txn_type)) txn_type_list
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
