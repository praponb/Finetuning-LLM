SELECT status FROM orders WHERE discount < 0.16 UNION ALL SELECT status FROM payments WHERE method IN ('cash', 'promptpay', 'bank_transfer');
