SELECT txn_time, WEEKOFYEAR(txn_time) AS txn_time_weekofyear, CAST(amount AS INT) AS amount_int
FROM transactions
WHERE txn_type = 'CREDIT'
ORDER BY txn_time_weekofyear DESC;
