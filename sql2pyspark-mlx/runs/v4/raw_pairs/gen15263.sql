SELECT * FROM (SELECT order_id, method, amount FROM payments) src PIVOT (MAX(amount) AS amount, COUNT(1) AS cnt FOR method IN ('promptpay', 'cash', 'paypal'));
