SELECT id, amount, order_id
FROM payments
WHERE amount > (SELECT MAX(amount) FROM payments WHERE method = 'promptpay')
ORDER BY order_id
