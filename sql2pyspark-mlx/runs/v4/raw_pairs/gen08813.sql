SELECT * FROM (SELECT status, method, amount FROM payments)
PIVOT (MAX(amount) FOR method IN ('card', 'promptpay', 'bank_transfer'))
ORDER BY status
