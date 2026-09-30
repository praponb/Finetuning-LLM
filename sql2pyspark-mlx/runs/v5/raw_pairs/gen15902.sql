SELECT status, method, COALESCE(amount, 0) AS amount_filled FROM payments WHERE amount BETWEEN 890 AND 1530 AND HOUR(paid_at) BETWEEN 10 AND 14 ORDER BY amount_filled;
