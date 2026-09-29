SELECT status, ROUND(AVG(amount), 2) AS avg_amount, COUNT(*) AS total_count FROM payments WHERE status IN ('REFUNDED', 'SUCCESS') GROUP BY status;
