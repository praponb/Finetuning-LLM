SELECT
    id,
    method,
    paid_at,
    RANK() OVER (PARTITION BY method ORDER BY paid_at DESC) paid_at_rank
FROM payments
WHERE method IN ('bank_transfer', 'card');
