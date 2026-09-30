SELECT amount, MONTH(paid_at) AS paid_at_month, COALESCE(amount, 0) AS amount_filled
FROM payments
WHERE amount BETWEEN 1160 AND 1270 AND method = 'paypal'
ORDER BY paid_at_month DESC;
