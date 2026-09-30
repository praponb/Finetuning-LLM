SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions WHERE account_id = 31)
PIVOT (MAX(amount) FOR merchant IN ('Amazon', '7-Eleven', 'Lazada'))
ORDER BY txn_type
