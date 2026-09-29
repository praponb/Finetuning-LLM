SELECT id, method, amount, RANK() OVER (PARTITION BY method ORDER BY amount) AS amount_rank FROM payments;
