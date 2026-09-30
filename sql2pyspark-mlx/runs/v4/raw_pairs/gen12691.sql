SELECT * FROM (SELECT is_flagged, txn_type, amount FROM transactions) PIVOT (COUNT(amount) FOR txn_type IN ('FEE', 'REFUND', 'DEBIT', 'CREDIT')) ORDER BY is_flagged;
