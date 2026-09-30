SELECT
    txn_type,
    QUARTER(txn_time) AS txn_quarter,
    account_id,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    ROUND(STDDEV(amount), 2) AS stddev_amount
FROM transactions
GROUP BY txn_type, QUARTER(txn_time), account_id
