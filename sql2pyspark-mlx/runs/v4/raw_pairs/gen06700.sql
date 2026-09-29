SELECT merchant, SUM(amount) AS total_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
FROM transactions
GROUP BY merchant
ORDER BY txn_type_values, total_amount
