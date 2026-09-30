SELECT amount, HOUR(txn_time) AS txn_time_hour FROM transactions WHERE TO_DATE(txn_time) < '2023-03-01' AND amount BETWEEN 2500 AND 2700;
