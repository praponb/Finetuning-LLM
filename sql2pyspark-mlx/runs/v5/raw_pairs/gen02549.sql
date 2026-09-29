SELECT * FROM (SELECT city, segment, credit_limit FROM customers) src PIVOT (SUM(credit_limit) AS amount FOR segment IN ('Consumer' AS consumer, 'SMB' AS smb));
