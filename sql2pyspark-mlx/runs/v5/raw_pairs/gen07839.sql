SELECT status, method, paid_at
FROM payments
WHERE method NOT IN ('card', 'promptpay', 'paypal')
