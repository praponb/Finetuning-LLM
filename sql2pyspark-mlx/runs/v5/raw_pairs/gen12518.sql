SELECT status, MIN(amount) AS min_amount, SUM(CASE WHEN status = 'FAILED' THEN amount ELSE 0 END) AS failed_amount, COUNT(amount) AS amount_count FROM payments GROUP BY status ORDER BY amount_count
