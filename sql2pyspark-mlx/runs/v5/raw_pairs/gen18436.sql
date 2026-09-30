SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions WHERE account_id IN (1, 11)) PIVOT (COUNT(amount) FOR merchant IN ('Amazon', 'Grab', 'Shell', 'Starbucks'))
