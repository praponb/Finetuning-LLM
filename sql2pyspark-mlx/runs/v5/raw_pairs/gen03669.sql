SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions) src PIVOT (SUM(amount) FOR merchant IN ('7-Eleven' AS v_7_eleven, 'Amazon' AS amazon));
