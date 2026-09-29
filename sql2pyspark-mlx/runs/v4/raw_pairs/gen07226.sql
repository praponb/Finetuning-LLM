SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions WHERE account_id IN (6, 10, 24, 38))
PIVOT (MAX(amount) FOR merchant IN ('Lazada', 'Amazon', '7-Eleven'))
ORDER BY txn_type;
