SELECT status, amount
FROM payments
WHERE paid_at < '2025-07-01' AND status NOT IN ('SUCCESS', 'FAILED')
ORDER BY amount DESC
