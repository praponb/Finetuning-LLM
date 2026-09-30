SELECT * FROM (SELECT account_id, txn_type, amount FROM transactions) src PIVOT (MAX(amount) AS amount, COUNT(1) AS n FOR txn_type IN ('DEBIT', 'CREDIT', 'REFUND'));
