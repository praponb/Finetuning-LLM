SELECT * FROM payments WHERE HOUR(paid_at) BETWEEN 12 AND 15 AND method IN ('cash', 'bank_transfer', 'promptpay')
