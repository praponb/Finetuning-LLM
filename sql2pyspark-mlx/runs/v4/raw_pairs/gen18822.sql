SELECT * FROM (SELECT order_id, method, status, amount FROM payments) src PIVOT (AVG(amount) AS amount, COUNT(1) AS cnt FOR status IN ('REFUNDED' AS refunded, 'FAILED' AS failed))
