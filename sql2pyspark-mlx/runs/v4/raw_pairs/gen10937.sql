SELECT method, MAX(paid_at) AS last_paid_at, COUNT(CASE WHEN method = 'paypal' THEN 1 END) AS paypal_count FROM payments WHERE order_id IN (17, 22, 27, 32) OR amount < 1030 GROUP BY method;
