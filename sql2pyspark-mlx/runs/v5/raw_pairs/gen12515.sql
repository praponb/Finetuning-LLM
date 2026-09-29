WITH gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
sessions_payments AS (
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    id,
    amount,
    burst_id
FROM sessions_payments
