SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions) src PIVOT (SUM(amount) AS amount, COUNT(1) AS cnt FOR merchant IN ('Grab' AS grab, 'Amazon' AS amazon));
