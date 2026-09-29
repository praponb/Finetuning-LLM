WITH enriched AS (
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY status, order_id ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_amount,
        MIN(amount) OVER (PARTITION BY status, order_id ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_amount,
        COUNT(amount) OVER (PARTITION BY status, order_id ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_amount,
        MAX(amount) OVER (PARTITION BY status, order_id ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_amount
    FROM payments
)
SELECT
    id,
    order_id,
    paid_at,
    status,
    last4_min_amount,
    last4_max_amount
FROM enriched
ORDER BY last4_max_amount DESC;
