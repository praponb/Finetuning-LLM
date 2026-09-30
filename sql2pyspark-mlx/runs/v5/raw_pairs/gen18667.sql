SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions) PIVOT (MAX(amount) FOR merchant IN ('Amazon', 'Starbucks')) ORDER BY txn_type;
