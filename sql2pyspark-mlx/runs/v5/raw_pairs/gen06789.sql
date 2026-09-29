SELECT
    id,
    status,
    paid_at,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY paid_at DESC) AS paid_at_pct_rank
FROM payments
WHERE TO_DATE(paid_at) < '2025-09-01'
ORDER BY 1 DESC
