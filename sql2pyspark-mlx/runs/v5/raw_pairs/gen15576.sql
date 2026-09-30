SELECT * FROM (SELECT merchant, txn_type, amount FROM transactions) PIVOT (COUNT(amount) FOR txn_type IN ('FEE', 'CREDIT', 'REFUND', 'DEBIT')) ORDER BY merchant;
