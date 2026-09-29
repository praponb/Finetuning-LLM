SELECT account_id, merchant, TO_DATE(txn_time) AS txn_time_date
FROM transactions
