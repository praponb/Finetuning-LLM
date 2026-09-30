SELECT *, CASE WHEN method = 'paypal' THEN 1 ELSE 0 END AS is_paypal FROM payments WHERE paid_at >= '2024-10-01';
