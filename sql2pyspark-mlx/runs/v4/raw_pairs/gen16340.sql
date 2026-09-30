SELECT account_id, merchant, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list FROM transactions GROUP BY account_id, merchant ORDER BY account_id DESC, merchant;
