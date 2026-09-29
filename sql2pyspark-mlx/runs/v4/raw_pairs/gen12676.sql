SELECT YEAR(txn_time) AS txn_year, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
FROM transactions
WHERE amount <= 1400
GROUP BY YEAR(txn_time);
