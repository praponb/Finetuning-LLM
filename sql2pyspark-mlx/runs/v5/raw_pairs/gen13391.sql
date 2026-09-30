SELECT txn_type, QUARTER(txn_time) txn_quarter, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) txn_type_values
FROM transactions
GROUP BY txn_type, QUARTER(txn_time)
ORDER BY txn_type_values;
