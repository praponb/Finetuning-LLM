WITH enriched AS (
    SELECT
        id,
        method,
        amount,
        AVG(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
        MIN(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount,
        MAX(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount
    FROM payments
)
SELECT
    id,
    amount,
    running_avg_amount,
    running_min_amount,
    running_max_amount
FROM enriched
ORDER BY running_avg_amount DESC
