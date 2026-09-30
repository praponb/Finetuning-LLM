SELECT * FROM (SELECT account_id, txn_type, amount FROM transactions) src PIVOT (MAX(amount) FOR txn_type IN ('FEE', 'REFUND'));
