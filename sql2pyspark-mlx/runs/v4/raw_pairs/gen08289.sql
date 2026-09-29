SELECT * FROM (SELECT order_id, method, amount FROM payments)
PIVOT (SUM(amount) FOR method IN ('promptpay', 'card'));
