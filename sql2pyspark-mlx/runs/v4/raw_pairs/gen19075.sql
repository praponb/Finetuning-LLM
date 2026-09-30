SELECT * FROM (SELECT status, method, amount FROM payments)
PIVOT (AVG(amount) FOR method IN ('card', 'paypal', 'cash', 'bank_transfer'))
ORDER BY status
