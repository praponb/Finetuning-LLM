SELECT SUM(amount) AS total_amount FROM payments WHERE order_id IN (6, 17, 38, 40) AND amount <= 1460 AND HOUR(paid_at) BETWEEN 11 AND 18 ORDER BY total_amount DESC;
