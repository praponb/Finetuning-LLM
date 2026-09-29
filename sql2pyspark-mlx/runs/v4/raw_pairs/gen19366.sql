SELECT txn_type, MIN(amount) AS min_amount FROM transactions WHERE account_id = 28 AND (txn_time >= '2023-06-24' OR HOUR(txn_time) BETWEEN 7 AND 10) GROUP BY 1;
