SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions)
PIVOT (COUNT(amount) FOR merchant IN ('7-Eleven', 'Shell', 'Lazada', 'Grab'))
