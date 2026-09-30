SELECT id, amount, paid_at, CASE WHEN method = 'paypal' THEN 1 ELSE 0 END AS is_paypal FROM payments WHERE amount > 700 AND status IN ('FAILED', 'SUCCESS');
