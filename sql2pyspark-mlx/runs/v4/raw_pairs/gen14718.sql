SELECT amount, DAYOFWEEK(txn_time) AS txn_time_dayofweek, CAST(amount AS INT) AS amount_int
FROM transactions
WHERE merchant IN ('Grab', '7-Eleven') AND txn_type <> 'DEBIT'
