SELECT txn_time
FROM transactions
WHERE TO_DATE(txn_time) >= '2023-08-10' AND NOT is_flagged AND merchant IN ('Starbucks', 'Lazada');
