SELECT id, status, amount, MIN(amount) OVER (PARTITION BY status) AS status_min_amount FROM payments WHERE paid_at < '2023-11-01' ORDER BY amount DESC
