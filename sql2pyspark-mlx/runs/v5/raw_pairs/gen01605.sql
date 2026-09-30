SELECT txn_type, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list FROM transactions WHERE txn_type IN ('FEE', 'DEBIT') GROUP BY txn_type ORDER BY txn_type DESC LIMIT 5
