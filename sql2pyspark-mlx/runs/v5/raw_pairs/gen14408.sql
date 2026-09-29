WITH with_windows AS (
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY method, order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
        SUM(amount) OVER (PARTITION BY method, order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount,
        MAX(amount) OVER (PARTITION BY method, order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount
    FROM payments
),
ranked AS (
    SELECT
        id,
        method,
        paid_at,
        status,
        running_avg_amount,
        running_sum_amount,
        running_max_amount
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY paid_at DESC, id) = 1
),
enriched AS (
    SELECT
        *,
        LEAD(running_sum_amount, 1) OVER w_running AS next_running_sum_amount,
        LAST_VALUE(running_max_amount) IGNORE NULLS OVER (PARTITION BY status, method ORDER BY running_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_max_amount,
        NTILE(10) OVER (PARTITION BY method, status ORDER BY running_max_amount DESC, id) AS running_max_amount_bucket,
        NTILE(4) OVER w_running AS running_avg_amount_bucket
    FROM ranked
    WHERE status = 'REFUNDED'
    WINDOW w_running AS (PARTITION BY method ORDER BY running_avg_amount, id)
),
with_windows_payments AS (
    SELECT
        id,
        status,
        running_max_amount,
        next_running_sum_amount,
        running_max_amount_bucket,
        LAG(next_running_sum_amount, 2) OVER (ORDER BY running_max_amount, id) AS prev_next_running_sum_amount,
        ROW_NUMBER() OVER w_part AS event_seq,
        COUNT(running_sum_amount) OVER (PARTITION BY method) AS method_count_running_sum_amount
    FROM enriched
    WINDOW w_part AS (ORDER BY running_avg_amount, id)
),
deduped AS (
    SELECT
        id,
        status,
        running_max_amount,
        next_running_sum_amount,
        prev_next_running_sum_amount,
        event_seq,
        method_count_running_sum_amount
    FROM with_windows_payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY running_max_amount_bucket DESC, id) = 1
)
SELECT
    CASE WHEN prev_next_running_sum_amount >= 8900 THEN 'LARGE' ELSE 'SMALL' END AS prev_next_running_sum_amount_tier,
    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_rows,
    ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END), 0), 2) AS event_seq_per_success
FROM deduped
GROUP BY 1
ORDER BY failed_rows DESC;
