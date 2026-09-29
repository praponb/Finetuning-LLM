SELECT status
FROM payments
WHERE amount >= 1240
UNION
SELECT status
FROM payments
WHERE method IN ('promptpay', 'bank_transfer', 'paypal');
