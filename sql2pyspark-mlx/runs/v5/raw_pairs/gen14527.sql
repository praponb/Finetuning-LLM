SELECT paid_at, amount, status, TO_DATE(paid_at) AS paid_at_date FROM payments WHERE (amount >= 970 AND method = 'bank_transfer') OR status = 'FAILED' ORDER BY status, paid_at_date;
