SELECT * FROM (SELECT order_id, status, method, amount FROM payments) src PIVOT (SUM(amount) AS total FOR method IN ('card', 'promptpay'));
