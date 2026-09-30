SELECT * FROM (SELECT merchant, txn_type, amount FROM transactions) src PIVOT (AVG(amount) AS value_sum, COUNT(1) AS cnt FOR txn_type IN ('DEBIT', 'FEE'));
