SELECT id, txn_time, TO_DATE(txn_time) AS txn_time_date
FROM transactions
WHERE is_flagged = TRUE OR txn_type = 'CREDIT';
