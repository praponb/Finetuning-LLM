SELECT DISTINCT status, method
FROM payments
WHERE method IN ('paypal', 'promptpay', 'bank_transfer')
