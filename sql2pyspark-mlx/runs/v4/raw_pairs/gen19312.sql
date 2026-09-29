SELECT status, 'account' AS source
FROM accounts
WHERE MONTH(opened_date) IN (6, 7, 10)
UNION ALL
SELECT status, 'payment' AS source
FROM payments
WHERE method IN ('bank_transfer', 'cash', 'paypal');
