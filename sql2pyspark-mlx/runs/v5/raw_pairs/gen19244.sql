WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS rn
    FROM payments
    QUALIFY rn = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    MAX(amount) AS max_amount,
    AVG(amount) FILTER (WHERE status = 'FAILED') AS failed_avg_amount,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
    MIN(paid_at) AS first_paid_at
FROM deduped
GROUP BY DATE_TRUNC('YEAR', paid_at)
