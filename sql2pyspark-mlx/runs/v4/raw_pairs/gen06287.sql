SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions) src PIVOT (AVG(amount) FOR merchant IN ('Lazada' AS lazada, 'Starbucks' AS starbucks, '7-Eleven' AS v_7_eleven))
