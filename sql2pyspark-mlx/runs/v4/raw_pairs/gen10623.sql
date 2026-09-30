SELECT * FROM (SELECT account_id, txn_type, amount FROM transactions WHERE txn_type = 'FEE')
PIVOT (MAX(amount) FOR txn_type IN ('REFUND', 'DEBIT', 'FEE', 'CREDIT'));
