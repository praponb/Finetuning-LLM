SELECT status, MAX(amount) AS max_amount, MIN(paid_at) AS first_paid_at FROM payments GROUP BY status HAVING max_amount >= 940
