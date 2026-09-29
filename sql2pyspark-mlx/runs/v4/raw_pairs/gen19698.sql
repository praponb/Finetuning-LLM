SELECT * FROM (SELECT account_id, merchant, txn_type, amount FROM transactions) src PIVOT (MAX(amount) AS amount, COUNT(1) AS n FOR txn_type IN ('DEBIT' AS debit, 'CREDIT' AS credit, 'FEE' AS fee))
