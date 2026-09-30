SELECT id, merchant FROM transactions WHERE amount BETWEEN 1700 AND 3500 UNION ALL SELECT id, merchant FROM transactions WHERE TO_DATE(txn_time) < '2023-11-01';
