SELECT * FROM (SELECT method, status, amount FROM payments) src PIVOT (MAX(amount) AS value_sum, COUNT(1) AS cnt FOR status IN ('REFUNDED', 'FAILED'));
