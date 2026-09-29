SELECT
    status,
    status_group,
    avg_amount,
    first_paid_at
FROM (
    SELECT
        status,
        CASE WHEN status IN ('FAILED', 'REFUNDED') THEN 'priority' ELSE 'other' END status_group,
        ROUND(AVG(amount), 2) avg_amount,
        MIN(paid_at) first_paid_at
    FROM payments
    GROUP BY status, CASE WHEN status IN ('FAILED', 'REFUNDED') THEN 'priority' ELSE 'other' END
) agg
WHERE avg_amount > 31
ORDER BY first_paid_at, 2;
