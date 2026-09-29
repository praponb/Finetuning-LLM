SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions) PIVOT (COUNT(amount) FOR merchant IN ('Grab', 'Amazon', 'Starbucks'));
