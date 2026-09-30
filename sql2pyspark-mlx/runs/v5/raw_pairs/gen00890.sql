SELECT * FROM (SELECT merchant, txn_type, amount FROM transactions) PIVOT (COUNT(amount) FOR txn_type IN ('REFUND', 'CREDIT')) ORDER BY merchant
