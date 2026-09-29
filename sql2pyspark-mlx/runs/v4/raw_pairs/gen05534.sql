SELECT * FROM (SELECT city, country, segment, credit_limit FROM customers) src PIVOT (MAX(credit_limit) AS amount, COUNT(1) AS cnt FOR segment IN ('Consumer' AS consumer, 'Corporate' AS corporate));
