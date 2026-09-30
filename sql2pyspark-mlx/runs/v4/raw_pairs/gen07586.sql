SELECT txn_type, MIN(amount) AS min_amount
FROM transactions
GROUP BY txn_type;
