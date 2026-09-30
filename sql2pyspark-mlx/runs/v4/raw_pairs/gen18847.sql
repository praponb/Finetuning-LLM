SELECT
    id,
    status,
    paid_at,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY paid_at DESC) AS paid_at_pct_rank
FROM payments;
