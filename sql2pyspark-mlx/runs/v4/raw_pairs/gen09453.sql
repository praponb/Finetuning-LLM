SELECT status, order_id, ROUND(AVG(amount), 2) AS avg_amount FROM payments WHERE (HOUR(paid_at) BETWEEN 10 AND 15 AND status = 'SUCCESS') OR order_id IS NULL GROUP BY 1, 2;
