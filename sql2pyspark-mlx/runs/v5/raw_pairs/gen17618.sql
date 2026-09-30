WITH enriched AS (
    SELECT
        id,
        order_id,
        MAX(amount) OVER (PARTITION BY method, order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
        SUM(amount) OVER (PARTITION BY method, order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount,
        COUNT(amount) OVER (PARTITION BY method, order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount
    FROM payments
    WHERE paid_at >= '2025-02-06'
)
SELECT
    id,
    running_max_amount,
    running_sum_amount,
    running_count_amount
FROM enriched;
