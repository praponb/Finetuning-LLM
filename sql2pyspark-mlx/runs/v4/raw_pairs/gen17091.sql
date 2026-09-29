SELECT amount, method, CASE WHEN method = 'paypal' THEN 1 ELSE 0 END is_paypal FROM payments WHERE amount <= 980;
