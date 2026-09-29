SELECT * FROM (SELECT account_id, merchant, txn_type, amount FROM transactions) src PIVOT (MAX(amount) AS value_sum, COUNT(1) AS cnt FOR txn_type IN ('FEE', 'CREDIT'))
