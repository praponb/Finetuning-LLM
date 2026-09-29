SELECT id, txn_type, txn_time, NTILE(4) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS bucket FROM transactions WHERE merchant NOT IN ('Shell', 'Starbucks', '7-Eleven')
