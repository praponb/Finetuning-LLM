SELECT * FROM (SELECT status, SUM(CASE WHEN method = 'paypal' THEN 1 ELSE 0 END) AS paypal_count, ROUND(AVG(amount), 2) AS avg_amount FROM payments GROUP BY status) AS t WHERE paypal_count < 66;
