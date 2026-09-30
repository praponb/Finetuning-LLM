SELECT * FROM (SELECT merchant, txn_type, amount FROM transactions)
PIVOT (SUM(amount) FOR txn_type IN ('DEBIT', 'FEE', 'REFUND', 'CREDIT'))
ORDER BY merchant
