SELECT * FROM (SELECT merchant, txn_type, amount FROM transactions WHERE merchant IN ('Starbucks', 'Shell')) PIVOT (MAX(amount) FOR txn_type IN ('REFUND', 'FEE', 'CREDIT', 'DEBIT'))
